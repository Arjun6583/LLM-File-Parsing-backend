from langchain_community.embeddings import SentenceTransformerEmbeddings
from langchain_community.vectorstores import FAISS
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_core.documents import Document
from langchain_core.prompts import ChatPromptTemplate
from langchain_community.chat_models import ChatFireworks
from langchain_core.output_parsers import StrOutputParser
import pandas as pd
import json, os
from difflib import get_close_matches
from langchain_community.document_loaders import (
    TextLoader, CSVLoader, UnstructuredPDFLoader,
    UnstructuredWordDocumentLoader, UnstructuredExcelLoader
)
import filetype
import mimetypes
import pdfplumber
from docx import Document
import google.generativeai as genai

# Configure the API key
genai.configure(api_key="AIzaSyA-nIR5wXp2SZJoka7UafQvn1c5rmjEmhU")

# Initialize the model
model = genai.GenerativeModel(model_name="gemini-2.0-flash-exp")


text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=100)
schema_mapping = {
    "Player Name": ["p_name", "player", "name", "full_name", "player_name", "player_full_name", "player_full"],
    "Matches": ["mat", "match_count", "total_matches", "games", "total_games", "matches_played", "matches_count"],
    "Innings": ["inns", "innings_count", "played_innings", "total_innings", "innings_played"],
    "Total Runs": ["runs", "total_score", "r_total", "total_runs", "runs_scored", "total_runs_scored"],
    "Highest Score": ["hs", "max_score", "top_score", "highest", "highest_score"],
    "Average": ["ave", "bat_avg", "average_runs", "avg", "batting_average"],
    "Wickets": ["wkt", "wickets_taken", "total_wickets", "wicket_count"],
    "Centuries": ["100s", "centuries", "century", "100"],
    "Half Centuries": ["50s", "half_centuries", "50", "half_century", "fifty"],
    "Fifers": ["five_wkts", "5_wickets", "5", "fifer", "five_wicket_haul"],
    "Best Bowling Figures": ["bbf", "best_figures", "best_bowling", "best_bowling_figures", "best_bowling_figure"]
}

flat_map = {alias.lower(): key for key, aliases in schema_mapping.items() for alias in aliases}

def extract_tables_from_docx(file_path):
    doc = Document(file_path)
    tables = []

    for table in doc.tables:
        data = []
        for row in table.rows:
            data.append([cell.text.strip() for cell in row.cells])
        if data and len(data) > 1:
            df = pd.DataFrame(data[1:], columns=data[0])  # assumes first row is header
            tables.append(df)
    
    return pd.concat(tables, ignore_index=True) if tables else pd.DataFrame()


def load_pdf_tables_with_plumber(file_path):
    all_tables = []
    with pdfplumber.open(file_path) as pdf:
        for page in pdf.pages:
            page_tables = page.extract_tables()
            for table in page_tables:
                print(table)
                if not table or not table[0] or len(set(table[0])) < len(table[0]):
                    continue 
                try:
                    df = pd.DataFrame(table[1:], columns=table[0])
                    df.columns = [str(col).strip() for col in df.columns]
                    all_tables.append(df)
                except Exception:
                    continue
    return pd.concat(all_tables, ignore_index=True) if all_tables else pd.DataFrame()

def load_file(file_path):
    ext = os.path.splitext(file_path)[1].lower()
    if ext == ".csv":
        return CSVLoader(file_path).load(), "tabular"
    elif ext == ".xlsx":
        return UnstructuredExcelLoader(file_path).load(), "tabular"
    elif ext == ".pdf":
        return UnstructuredPDFLoader(file_path).load(), "document"
    elif ext == ".docx":
        return UnstructuredWordDocumentLoader(file_path).load(), "document"
    else:
        raise ValueError("Unsupported file type")

embedding = SentenceTransformerEmbeddings(model_name="all-MiniLM-L6-v2")

def get_context_with_rag(docs, query, k=3):
    text_splitter = RecursiveCharacterTextSplitter(chunk_size=500, chunk_overlap=50)
    texts = text_splitter.split_documents(docs)
    vector_store = FAISS.from_documents(texts, embedding)
    results = vector_store.similarity_search(query, k=k)
    return "\n\n".join([r.page_content for r in results])

# llm = ChatFireworks(
#     model="accounts/fireworks/models/deepseek-r1",
#     fireworks_api_key="fw_3ZNMWr1aUodwnyJcXMBDrvEZ",
#     temperature=0.3
# )

# prompt = ChatPromptTemplate.from_template("""
# You are a cricket data analyst. Given schema and extracted file text, match as many fields as possible.

# Schema (standard fields and aliases):
# {schema}

# Relevant Extracted Text:
# {context}

# Return JSON: {{ "matched_columns": {{}}, "unmatched_columns": [], "summary": "", }}
# """)

# chain = prompt | llm | StrOutputParser()

# def map_document_with_rag(docs):
#     context = get_context_with_rag(docs, query="cricket player stats")
#     input_data = {
#         "schema": "\n".join([f"{k}: {v}" for k, v in schema_mapping.items()]),
#         "context": context
#     }
#     return json.loads(chain.invoke(input_data))

def fuzzy_match_columns(headers):
    matched, unmatched = {}, []
    for col in headers:
        match = get_close_matches(col.lower(), flat_map.keys(), n=1, cutoff=0.7)
        if match:
            matched[col] = flat_map[match[0]]
        else:
            unmatched.append(col)
    return matched, unmatched

def process_tabular(file_path):
    df = pd.read_csv(file_path) if file_path.endswith(".csv") else pd.read_excel(file_path)
    print("Hi")
    print(df)
    #first 10 records of the dataframe
    context = df.head(10).to_string(index=False, header=True)
    headers = df.columns.tolist()
    try:
        schema = list(schema_mapping.keys())
        columns = [col.lower() for col in headers]
        prompt_tabular = f"""
        You are an expert cricket data analyst. Your task is to intelligently map each header column from the uploaded file to a known schema.

        ### Step-by-step instructions:

        1. Use the provided `context` (sample data from the file) to understand the meaning of each column.
        2. Compare each column name and its sample values with the predefined `schema` to find the best match.
        3. Return:
        - `matched_columns`: a JSON dictionary where keys are actual column names from the file and values are the corresponding schema field names.
        - `unmatched_columns`: a list of columns from the file that could not be confidently matched.
        - `summary`: a short explanation of your reasoning or any uncertainties.

        ### Context (sample data from the file):
        {context}

        ### Known Schema Fields:
        {schema}

        ### Column Headers to Map:
        {columns}

        ### Output JSON Format (strict):
        {{
        "matched_columns": {{
            "file_column_1": "schema_field_1",
            "file_column_2": "schema_field_2"
        }},
        "unmatched_columns": ["file_column_3"],
        "summary": "Mapped based on column names and values such as runs, matches, etc."
        }}
        """
        response = model.generate_content(
            prompt_tabular,
            generation_config={
                "temperature": 0.1,
                "max_output_tokens": 2000,
                "response_mime_type": "application/json"

            },
            safety_settings=[
                {"category": "HARM_CATEGORY_DEROGATORY", "threshold": 3},
                {"category": "HARM_CATEGORY_VIOLENCE", "threshold": 3},
                {"category": "HARM_CATEGORY_SEXUAL", "threshold": 3},
                {"category": "HARM_CATEGORY_MEDICAL", "threshold": 3},
                {"category": "HARM_CATEGORY_DANGEROUS", "threshold": 3},
            ]
        )
        print("\n-----------Result is-------\n", response.text)
    except:
        matched, unmatched = fuzzy_match_columns(headers)
    
    print("Unmatched Columns: ", unmatched)
    print("Matched Columns: ", matched)
    total = len(df)
    accepted = total if len(matched) >= 6 else 0
    matched_data = df[list(matched.keys())] if matched else pd.DataFrame()

    result = {
        "file_path": file_path,
        "total_records": total,
        "accepted_records": accepted,
        "rejected_records": total - accepted,
        "matched_columns": list(matched.keys()),
        "unmatched_columns": unmatched,
        "status": "Accepted" if accepted else "Rejected"
    }

    return result, matched_data, matched

# def process_document(docs, file_path=None):
#     if not file_path:
#         raise ValueError("PDF file_path is required")
#     if file_path.endswith(".docx"):
#         df = extract_tables_from_docx(file_path)
#     else:
#         df = load_pdf_tables_with_plumber(file_path)
#     if df.empty:
#         return {
#             "file_path": file_path,
#             "total_records": 0,
#             "accepted_records": 0,
#             "rejected_records": 0,
#             "matched_columns": [],
#             "unmatched_columns": [],
#             "status": "Rejected"
#         }, pd.DataFrame()

#     headers = df.columns.tolist()

#     # Use LLM mapping
#     try:
#         input_data = {
#             "schema": "\n".join([f"{k}: {v}" for k, v in schema_mapping.items()]),
#             "columns": headers
#         }
#         prompt_tabular = ChatPromptTemplate.from_template("""
#         You are a cricket analyst. Map the given headers to schema.

#         Schema:
#         {schema}

#         Headers:
#         {columns}

#         Return JSON: {{ "matched_columns": {{}}, "unmatched_columns": [], "summary": "", "matched_columns_schema": {{}} }}
#         """)
#         llm_chain = prompt_tabular | llm | StrOutputParser()
#         llm_result = json.loads(llm_chain.invoke(input_data))
#         print("\n-----------Result is-------\n", llm_result)
#         matched = llm_result.get("matched_columns", {})
#         unmatched = llm_result.get("unmatched_columns", headers)
#     except Exception:
#         matched, unmatched = fuzzy_match_columns(headers)

#     total = len(df)
#     print("Matched Columns: ", matched)
#     print("Unmatched Columns: ", unmatched)
#     accepted = total if len(matched) != 0 else 0
#     matched_data = df[list(matched.keys())] if matched else pd.DataFrame()

#     result = {
#         "file_path": file_path,
#         "total_records": total,
#         "accepted_records": accepted,
#         "rejected_records": total - accepted,
#         "matched_columns": list(matched.keys()),
#         "unmatched_columns": unmatched,
#         "status": "Accepted" if accepted else "Rejected"
#     }

#     return result, matched_data, matched


def detect_file_type(file_path):
    ext = os.path.splitext(file_path)[1].lower().strip('.') 
    if ext == "csv":
        return "text/csv", "csv"
    with open(file_path, 'rb') as f:
        kind = filetype.guess(f.read(262))
        if kind:
            if kind.extension == "zip":
                ext = os.path.splitext(file_path)[1].lower()
                mime, _ = mimetypes.guess_type(file_path)
                print("File Type: ", mime, ext)
                if "word" in mime:
                    print("This is docx file")
                else:
                    print("This is a pdf file")
                return mime or 'application/zip', ext.strip('.')
            return kind.mime, kind.extension
        return 'Unknown', 'Unknown'

def analyze_file(file_path):
    mime, ext = detect_file_type(file_path)

    if ext == "csv":
        result, matched_data = process_tabular(file_path)

    elif ext == "xlsx":
        try:
            df = pd.read_excel(file_path, engine='openpyxl')
            temp_csv = "temp_converted_from_excel.csv"
            df.to_csv(temp_csv, index=False)
            result, matched_data, matched = process_tabular(temp_csv)
            os.remove(temp_csv)  
        except Exception as e:
            print(f"[⚠️ Error reading Excel as table, trying as document] {e}")
            docs, _ = load_file(file_path)
            # result, matched_data = process_document(docs)

    elif "word" in mime:
        docs, _ = load_file(file_path)
        # result, matched_data, matched = process_document(docs, file_path=file_path)
    elif "pdf" in mime:
        docs, _ = load_file(file_path)
        # result, matched_data, matched = process_document(docs, file_path=file_path)
    else:
        raise ValueError(f"Unsupported or unknown file type: {mime} ({ext})")

    return result, matched_data, matched

#C:\Users\Dell\Downloads\test.pdf
file_path = r"C:\Users\Dell\Downloads\test.xlsx"
result, matched_data, matched = analyze_file(file_path)

matched_data.rename(columns=matched, inplace=True)

# # Ensure all standard columns exist
# for col in schema_mapping:
#     if col not in matched_data.columns:
#         matched_data[col] = "-"

# # Reorder columns
# matched_data = matched_data[list(schema_mapping.keys())]

# print("\n Final File Summary:")
# print(json.dumps(result, indent=2))

# print("\nMatched Rows:")
# print(matched_data.to_dict(orient="records"))



# Generate content with configuration




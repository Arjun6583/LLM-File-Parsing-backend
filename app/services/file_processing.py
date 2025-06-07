from langchain_text_splitters import RecursiveCharacterTextSplitter
import pandas as pd
import traceback
from services.file_parser import SCHEMA_MAPPING
from difflib import get_close_matches
from services.file_parser import extract_key_value_from_docx, extract_key_value_from_pdf
import google.generativeai as genai
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
from core.config import GEMENI_API_KEY, GEMENI_MODEL

genai.configure(api_key=GEMENI_API_KEY)

model = genai.GenerativeModel(model_name="gemini-2.0-flash-exp")

text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=100)
schema_mapping = SCHEMA_MAPPING
flat_map = {alias.lower(): key for key, aliases in schema_mapping.items() for alias in aliases}


model = SentenceTransformer("all-MiniLM-L6-v2")

def build_embedding_index(schema_mapping):
    """
    Builds an embedding index from schema aliases using a SentenceTransformer model.

    This function flattens the schema alias mapping into a lowercase list of all possible alias terms
    and generates their embeddings for later similarity comparisons during column mapping.

    Args:
        schema_mapping (dict): A dictionary where keys are canonical schema fields and values are lists of aliases.

    Returns:
        tuple:
            alias_list (list): Flattened list of all aliases in lowercase.
            embeddings (np.ndarray): Embeddings for the aliases using SentenceTransformer.
            flat_map (dict): Mapping of lowercase alias to canonical schema field.
    """
    flat_map = {alias.lower(): key for key, aliases in schema_mapping.items() for alias in aliases}
    alias_list = list(flat_map.keys())
    embeddings = model.encode(alias_list)
    return alias_list, embeddings, flat_map

def rag_lookup(column_headers, alias_list, alias_embeddings, flat_map, threshold=0.75):
    """
    Performs column matching using cosine similarity between file column headers and schema alias embeddings.

    Args:
        column_headers (list): List of headers from the uploaded file.
        alias_list (list): List of known aliases (lowercased).
        alias_embeddings (np.ndarray): Embeddings for the alias list.
        flat_map (dict): Mapping of alias (lowercase) to canonical schema field.
        threshold (float): Cosine similarity threshold to accept a match.

    Returns:
        tuple:
            matched (dict): File header to schema field mapping.
            unmatched (list): List of headers that could not be confidently matched.
    """
    matched, unmatched = {}, []
    input_embeddings = model.encode([col.lower() for col in column_headers])
    
    for idx, emb in enumerate(input_embeddings):
        sims = cosine_similarity([emb], alias_embeddings)[0]
        max_sim_idx = np.argmax(sims)
        if sims[max_sim_idx] >= threshold:
            matched[column_headers[idx]] = flat_map[alias_list[max_sim_idx]]
        else:
            unmatched.append(column_headers[idx])
    
    return matched, unmatched

def fuzzy_match_columns(headers):
    """
    Matches column headers to schema aliases using fuzzy string matching.

    Args:
        headers (list): List of headers to be matched.

    Returns:
        tuple:
            matched (dict): Header to schema field mapping based on best fuzzy match.
            unmatched (list): Headers that did not match any known alias above the cutoff.
    """
    matched, unmatched = {}, []
    for col in headers:
        match = get_close_matches(col.lower(), flat_map.keys(), n=1, cutoff=0.7)
        if match:
            matched[col] = flat_map[match[0]]
        else:
            unmatched.append(col)
    return matched, unmatched

def process_tabular(file_path):
    """
    Processes a CSV or Excel tabular file, maps headers to a known schema using LLM or fallback fuzzy logic,
    and returns structured results along with matched data.

    The function:
    Reads the uploaded tabular file
    Generates a prompt with sample data and schema
    Queries a Generative AI model to perform intelligent mapping
    Falls back to fuzzy logic if LLM fails
    Prepares and returns metadata and cleaned DataFrame

    Args:
        file_path (str): Path to the uploaded CSV/XLSX file.

    Returns:
        tuple:
            result (dict): Metadata about processing status, matched/unmatched columns, and record counts.
            matched_data (pd.DataFrame): DataFrame with renamed columns aligned to the schema.
            matched (dict): Mapping of original headers to schema fields.
    """
    try:
        df = pd.read_csv(file_path) if file_path.endswith(".csv") else pd.read_excel(file_path)
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

                }
            )
            matched = response.text["matched_columns"]
            unmatched = response.text["unmatched_columns"]
        except:
            matched, unmatched = fuzzy_match_columns(headers)
        
        total = len(df)
        accepted = total if len(matched) >= 6 else 0 
        mapped_temp_col = [key for key, _ in matched.items() if key in df.columns]
        matched_data = df[mapped_temp_col] if matched else pd.DataFrame()

        result = {
            "file_path": file_path,
            "total_records": total,
            "accepted_records": accepted,
            "rejected_records": total - accepted,
            "matched_columns": matched,
            "unmatched_columns": unmatched,
            "status": "Accepted" if accepted else "Rejected"
        }
        print(matched)
        return result, matched_data, matched

    except Exception as e:
        print(f"Tabular Processing Error {e}")
        traceback.print_exc()
        return {
            "file_path": file_path,
            "total_records": 0,
            "accepted_records": 0,
            "rejected_records": 0,
            "matched_columns": [],
            "unmatched_columns": [],
            "status": "Rejected"
        }, pd.DataFrame(), {}

def process_document(docs, file_path=None):
    """
    Processes a semi-structured document (PDF or DOCX), extracts tabular content,
    maps headers to schema using LLM or fuzzy logic, and returns structured metadata and cleaned data.

    Steps:
    Extract tabular data using DOCX/PDF parsers
    Generate LLM prompt from sample data and schema
    Attempt to match headers using the model
    Fallback to fuzzy matching if LLM fails
    Return analysis result and formatted data

    Args:
        docs: Currently unused; may be removed or reserved for future multi-page document support.
        file_path (str): File path to the DOCX or PDF file to process.

    Returns:
        tuple:
            result (dict): Metadata about file processing (status, matched/unmatched columns, record counts).
            matched_data (pd.DataFrame): Parsed DataFrame with schema-aligned column names.
            matched (dict): Mapping of file column names to schema field names.
    """
    try:
        if not file_path:
            raise ValueError("PDF file_path is required")
        if file_path.endswith(".docx"):
            df = extract_key_value_from_docx(file_path)
        else:
            df = extract_key_value_from_pdf(file_path)
        if df.empty:
            return {
                "file_path": file_path,
                "total_records": 0,
                "accepted_records": 0,
                "rejected_records": 0,
                "matched_columns": [],
                "unmatched_columns": [],
                "status": "Rejected"
            }, pd.DataFrame()
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

                }
            )
            matched = response.text["matched_columns"]
            unmatched = response.text["unmatched_columns"]
        except:
            matched, unmatched = fuzzy_match_columns(headers)
            
        total = len(df)
        accepted = total if len(matched) >= 6 else 0 
        mapped_temp_col = [key for key, _ in matched.items() if key in df.columns]
        matched_data = df[mapped_temp_col] if matched else pd.DataFrame()
        print(matched)
        result = {
            "file_path": file_path,
            "total_records": total,
            "accepted_records": accepted,
            "rejected_records": total - accepted,
            "matched_columns": matched,
            "unmatched_columns": unmatched,
            "status": "Accepted" if accepted else "Rejected"
        }
        return result, matched_data, matched
    except Exception as e:
        print(f"Document Processing Error {e}")
        traceback.print_exc()
        return {
            "file_path": file_path,
            "total_records": 0,
            "accepted_records": 0,
            "rejected_records": 0,
            "matched_columns": [],
            "unmatched_columns": [],
            "status": "Rejected"
        }, pd.DataFrame(), {}
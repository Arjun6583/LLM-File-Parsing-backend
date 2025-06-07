from services.file_processing import process_tabular, process_document
from services.file_loader import load_file, detect_file_type
import pandas as pd
import os

def analyze_file(file_path):
    """
    Analyzes a file based on its type and processes it accordingly.
    This function detects the file type, loads the file, and processes it as either a tabular or document format.

    Args:
        file_path (str): Path to the file to be analyzed.
    Returns:
        tuple: (result, matched_data, matched)
            result (dict): Summary of the file processing result.
            matched_data (pd.DataFrame): DataFrame containing matched columns and data.
            matched (list): List of matched headers and their corresponding schema fields.
    Raises:
        RuntimeError: If the file type is unsupported or if any processing step fails.
        ValueError: If the file type is unknown or unsupported.
    """
    try:
        mime, ext = detect_file_type(file_path)

        if ext == "csv":
            result, matched_data, matched = process_tabular(file_path)

        elif ext == "xlsx":
            try:
                df = pd.read_excel(file_path, engine='openpyxl')
                temp_csv = "temp_converted_from_excel.csv"
                df.to_csv(temp_csv, index=False)
                result, matched_data, matched = process_tabular(temp_csv)
                os.remove(temp_csv)
            except Exception as e:
                print(f"[Warning] Failed to read Excel as table. Trying as document: {e}")
                try:
                    docs, _ = load_file(file_path)
                    result, matched_data, matched = process_document(docs, file_path=file_path)
                except Exception as doc_e:
                    raise RuntimeError(f"Failed to process Excel file as document: {doc_e}")

        elif "word" in mime or ext in ["docx", "doc"]:
            try:
                docs, _ = load_file(file_path)
                result, matched_data, matched = process_document(docs, file_path=file_path)
            except Exception as e:
                raise RuntimeError(f"Failed to process Word document: {e}")

        elif "pdf" in mime or ext == "pdf":
            try:
                docs, _ = load_file(file_path)
                result, matched_data, matched = process_document(docs, file_path=file_path)
            except Exception as e:
                raise RuntimeError(f"Failed to process PDF document: {e}")

        else:
            raise ValueError(f"Unsupported or unknown file type: {mime} ({ext})")
        print("In File Analyzer: \n")
        return result, matched_data, matched

    except Exception as final_error:
        raise RuntimeError(f"File analysis failed: {final_error}")

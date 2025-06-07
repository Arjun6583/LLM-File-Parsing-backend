import os
import mimetypes
import filetype
from langchain_community.document_loaders import (
    CSVLoader, UnstructuredPDFLoader,
    UnstructuredWordDocumentLoader, UnstructuredExcelLoader
)

def detect_file_type(file_path):
    """
    Detects the MIME type and extension of a file.
    Uses the `filetype` library for binary file type detection,

    args:
        file_path (str): Path to the file to be analyzed.
    returns: 
        tuple: (MIME type, file extension)
    raises:
        RuntimeError: If file type detection fails or is unsupported.
    """
    try:
        ext = os.path.splitext(file_path)[1].lower().strip('.')
        if ext == "csv":
            return "text/csv", "csv"

        with open(file_path, 'rb') as f:
            kind = filetype.guess(f.read(262))
            if kind:
                if kind.extension == "zip":
                    mime, _ = mimetypes.guess_type(file_path)
                    print("File Type Detected:", mime, ext)
                    return mime or 'application/zip', ext
                return kind.mime, kind.extension
            else:
                return 'Unknown', 'Unknown'
    except Exception as e:
        raise RuntimeError(f"Failed to detect file type: {e}")


def load_file(file_path):
    """
    Loads a file based on its extension and returns its content and type.
    args:
        file_path (str): Path to the file to be loaded.
    returns:
        tuple: (content, type) where content is the loaded data and type is either 'tabular' or 'document'.
    raises:
        RuntimeError: If the file type is unsupported or loading fails.
    """
    ext = os.path.splitext(file_path)[1].lower()
    try:
        if ext == ".csv":
            return CSVLoader(file_path).load(), "tabular"
        elif ext == ".xlsx":
            return UnstructuredExcelLoader(file_path).load(), "tabular"
        elif ext == ".pdf":
            return UnstructuredPDFLoader(file_path).load(), "document"
        elif ext == ".docx":
            return UnstructuredWordDocumentLoader(file_path).load(), "document"
        else:
            raise ValueError("Unsupported file type for loading.")
    except Exception as e:
        raise RuntimeError(f"Failed to load file '{file_path}': {e}")

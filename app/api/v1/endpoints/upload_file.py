from fastapi import APIRouter, Depends, HTTPException, File, UploadFile
from sqlalchemy.orm import Session
import os
from services.file_analyzer import analyze_file
import shutil
import uuid
from services.save_data import save_file_analysis_to_db, save_rejected_files
from services.file_processing import schema_mapping
from db.session import get_db
router = APIRouter()

@router.post("/upload")
async def upload_file(file: UploadFile = File(...), db: Session = Depends(get_db)):
    """
    Upload and analyze a file, perform column mapping, and store the processed data and metadata in the database.

    This endpoint accepts a file (CSV, XLSX, PDF, DOCX), stores it with a unique name, analyzes the contents
    using column-mapping logic (via `analyze_file()`), and stores the structured data in the database.
    
    Steps performed:
    - Save uploaded file with a unique filename to local directory (`uploaded_files/`)
    - Analyze file content and attempt to map columns to a predefined schema using LLM
    - Rename matched columns and fill in missing schema fields with placeholders
    - Detect missing columns (those from schema not found in actual data)
    - Store structured data and metadata in the database using `save_file_analysis_to_db()`

    Parameters:
        file (UploadFile): The uploaded file from the client (CSV, XLSX, PDF, DOCX).
        db (Session): SQLAlchemy session dependency for database operations.

    Returns:
        dict: A dictionary containing:
            - file_id (int): Unique ID of the saved file in the database.
            - unique_filename (str): Generated unique filename stored on the server.
            - stored_path (str): Local path to the stored file.
            - matched_columns (dict): Dictionary mapping file headers to schema fields.
            - unmatched_columns (list): List of file columns that couldn't be matched.
            - file_name (str): Original filename uploaded by the user.
            - file_type (str): File extension/type of the uploaded file.
            - total_records (int): Total records parsed from the file.
            - missing_columns (list): List of expected schema columns that were not present in the file.

    Raises:
        HTTPException: If any error occurs during file upload, processing, or analysis, a 500 error is returned with details.
    """
    try:
        upload_dir = "uploaded_files"
        os.makedirs(upload_dir, exist_ok=True)

        file_ext = file.filename.split(".")[-1]
        if file_ext not in ["csv", "xlsx", "pdf", "docx"]:
            save_rejected_files(file_name=file.filename)
            raise HTTPException(status_code=400, detail="Unsupported file type. Only CSV, XLSX, PDF, and DOCX are allowed.")
        unique_filename = f"{uuid.uuid4().hex}.{file_ext}"
        stored_path = os.path.join(upload_dir, unique_filename)

        with open(stored_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
            
        result, matched_data, matched = await analyze_file(stored_path)
        
        if result["matched_columns"] == {}:
            save_rejected_files(file_name=file.filename, db=db)
            raise HTTPException(status_code=400, detail="File analysis failed or no matched columns found.")
        matched_data = matched_data.copy()

        if isinstance(matched, list):
            matched = {item["header"]: item["schema"] for item in matched if "header" in item and "schema" in item}
        matched_data.rename(columns=matched, inplace=True)

        for col in schema_mapping:
            if col not in matched_data.columns:
                matched_data[col] = "-"
        matched_data = matched_data[list(schema_mapping.keys())]
        # os.remove(temp_path)
        file_name = result["file_path"].split("/")[-1] if "file_path" in result else file.filename
        file_type = file.filename.split(".")[-1] if "." in file_name else "unknown"

        matched_data = matched_data.fillna("")

        t = matched_data.to_dict(orient="records")[0]
        col = {key for key, val in t.items() if isinstance(val, (str, int, float)) and val != "-"}
        missing_columns = [key for key in schema_mapping.keys() if key not in col]
        
        file_id = await save_file_analysis_to_db(file_name=file.filename, file_type=file_type, status=result["status"], total_records=result["total_records"], unmatched_columns=result["unmatched_columns"], matched_columns=result["matched_columns"], mapped_data=matched_data.to_dict(orient="records"), missing_columns=missing_columns, db = db)
        
        return {
            "file_id": file_id,
            "unique_filename": unique_filename,
            "stored_path": stored_path,
            "matched_columns": matched,
            "unmatched_columns": result["unmatched_columns"],
            "file_name": file.filename,
            "file_type": file_type,
            "total_records": result["total_records"],
            "missing_columns": missing_columns,
        }
    except Exception as e:
        print(f"Error processing file: {str(e)}")
        save_rejected_files(file_name=file.filename, db=db)
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")
    finally:
        pass
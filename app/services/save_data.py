from sqlalchemy.orm import Session
from db.file_model import FileUpload, MappedData, FileProcessingLog , Player, Batting, Bowling
import pandas as pd 
from services.file_parser import extract_key_value_from_docx, extract_key_value_from_pdf
from services.file_processing import SCHEMA_MAPPING as schema_mapping
from fastapi import HTTPException  

def save_file_analysis_to_db(
    file_name: str,
    file_type: str,
    matched_columns: list[str],
    unmatched_columns: list[str],
    total_records: int,
    status: str,
    mapped_data: list[dict],
    missing_columns: list[str],
    db: Session
):
    """
    Saves metadata and mapped content of a parsed file into the database.

    This function inserts a new file entry into the `FileUpload` table,
    stores the processing summary into the `FileProcessingLog` table,
    and saves the structured mapped data into the `MappedData` table.

    Args:
        file_name (str): The original name of the uploaded file.
        file_type (str): The type/extension of the file (e.g., csv, xlsx, pdf).
        matched_columns (list[str]): List of column names successfully mapped to schema fields.
        unmatched_columns (list[str]): List of column names that couldn't be matched.
        total_records (int): Total number of records parsed from the file.
        status (str): Final processing status (e.g., 'Accepted' or 'Rejected').
        mapped_data (list[dict]): List of dictionaries containing parsed and mapped row data.
        missing_columns (list[str]): List of schema fields missing from the uploaded data.
        db (Session): SQLAlchemy database session object.

    Returns:
        int: The database ID (`file_id`) of the inserted file record.

    Raises:
        Exception: Rolls back the transaction and re-raises any database error encountered.
    """
    try:
        file_record = FileUpload(file_name=file_name, file_type=file_type)
        db.add(file_record)
        db.flush() 

        metadata_record = FileProcessingLog(
            file_id=file_record.id,
            matched_columns=matched_columns,
            unmatched_columns=unmatched_columns,
            total_records=total_records,
            missing_columns=missing_columns,
            status=status,
        )
        db.add(metadata_record)

        mapped_data_record = MappedData(
            file_id=file_record.id,
            data=mapped_data
        )
        db.add(mapped_data_record)

        db.commit()
        db.refresh(file_record)
        return file_record.id

    except Exception as e:
        db.rollback()
        raise e 


def save_rejected_files(
    file_name: str,
    db: Session,
):
    """
    Saves metadata of a rejected file into the database.

    This function inserts a new file entry into the `FileUpload` table with a status of 'Rejected'.

    Args:
        file_name (str): The original name of the uploaded file.
        file_type (str): The type/extension of the file (e.g., csv, xlsx, pdf).
        status (str): Final processing status (should be 'Rejected').
        db (Session): SQLAlchemy database session object.

    Returns:
        int: The database ID (`file_id`) of the inserted file record.

    Raises:
        Exception: Rolls back the transaction and re-raises any database error encountered.
    """
    try:
        file_type = file_name.split(".")[-1] if "." in file_name else "unknown"
        file_name = file_name.split(".")[0]
        file_record = FileUpload(file_name=file_name, file_type=file_type)
        db.add(file_record)
        db.flush() 

        metadata_record = FileProcessingLog(
            file_id=file_record.id,
            status="rejected",
        )
        db.add(metadata_record)

        db.commit()
        db.refresh(file_record)
        return file_record.id

    except Exception as e:
        db.rollback()
        raise e 


def convert_string_to_int(value: str) -> int:
    """
    Convert a string to an integer, returning 0 if conversion fails.

    Parameters:
        value (str): The string value to convert.

    Returns:
        int: The converted integer or 0 if conversion fails.
    """
    try:
        return int(value)
    except ValueError:
        return None


def save_submit_columns_data(request, db: Session):
    """
    Submit column mappings and process the corresponding file data.

    This endpoint accepts user-provided column mappings and a file path, reads and transforms
    the file based on these mappings, and saves the structured data into the `Player`, 
    `Batting`, and `Bowling` tables. It also logs the results (matched/unmatched columns,
    total records, and status) in the `FileProcessingLog` table.

    Supported file types:
    - CSV
    - XLSX
    - PDF
    - DOCX

    Operations performed:
    - Reads file using pandas or appropriate extraction function.
    - Applies user-defined column mapping.
    - Normalizes columns to match schema.
    - Saves data into respective database tables.
    - Logs file processing result.

    Parameters:
        request (ColumnMappingRequest): Request body containing file path, type, mappings, and file ID.
        db (Session): SQLAlchemy database session provided by FastAPI dependency injection.

    Returns:
        dict: JSON response with a status and message indicating success or failure.
    """
    try:
        mapped = request.user_mapping
        print("User Selected: ", mapped)
        if request.file_type == 'csv' or request.file_type == 'xlsx':
             df = pd.read_csv(request.file_path) if request.file_type == "csv" else pd.read_excel(request.file_path)
 
        else:
            if request.file_path.endswith(".docx"):
                df = extract_key_value_from_docx(request.file_path)
            else:
                df = extract_key_value_from_pdf(request.file_path)
        
        header = df.columns.tolist() if isinstance(df, pd.DataFrame) else []
        print("Header: ", header)
        mapped_temp_col = []
        new_mapped = {}
        for key, value in mapped.items():
            new_key =  convert_string_to_int(key)
            if new_key != None and new_key in header:
                mapped_temp_col.append(new_key)
                new_mapped[new_key] = value
            elif key in header:
                mapped_temp_col.append(key)
                new_mapped[key] = value
        
        print("Mapped Temp Columns: ", mapped_temp_col)
        matched_data = df[mapped_temp_col] if mapped else pd.DataFrame()
        
        matched_data = matched_data.copy()
        matched_data.rename(columns=new_mapped, inplace=True)
        print("Matched Data Columns: ", matched_data.columns.tolist())

        for col in schema_mapping:
            if col not in matched_data.columns:
                matched_data[col] = "-"

        matched_data = matched_data[list(schema_mapping.keys())]
        matched_data = matched_data.fillna("")
        matched_data = matched_data.to_dict(orient="records")
        print("Matched Data: ", matched_data)
        file_id = request.file_id
        user_mapping = [value for _, value in request.user_mapping.items()]
        
        print(f"Received file_id: {file_id} with user_mapping: {user_mapping}")

        raw_data = matched_data
        final_data = []
        total_records = 0
        unmatched_columns_set = set()

        for row in raw_data:
            total_records += 1
            mapped_row = {}

            for mapped_key in user_mapping:
                value = row.get(mapped_key, None)
                if value is None:
                    unmatched_columns_set.add(mapped_key)
                mapped_row[mapped_key] = value

            final_data.append(mapped_row)

            # Insert into Player
            player = Player(
                file_id=file_id,
                player_name=mapped_row.get("Player Name", "Unknown"),
                matches=safe_int(mapped_row.get("Matches")),
                innings=safe_int(mapped_row.get("Innings")),
            )
            db.add(player)
            db.flush() 

            # Insert into Batting
            batting = Batting(
                player_id=player.id,
                total_runs=safe_int(mapped_row.get("Total Runs")),
                average=safe_float(mapped_row.get("Average")),
                highest_score=str(mapped_row.get("Highest Score") or "0"),
                century=safe_int(mapped_row.get("Centuries")),
                half_century=safe_int(mapped_row.get("Half Centuries")),
            )
            db.add(batting)

            # Insert into Bowling
            bowling = Bowling(
                player_id=player.id,
                wickets=safe_int(mapped_row.get("Wickets")),
                five_wickets=safe_int(mapped_row.get("Fifers")),
                best_bowling=str(mapped_row.get("Best Bowling Figures") or "0/0"),
            )
            db.add(bowling)

        file_log = db.query(FileProcessingLog).filter(FileProcessingLog.file_id == file_id).first()

        if not file_log:
            file_log = FileProcessingLog(
                file_id=file_id,
                matched_columns=list(user_mapping),
                unmatched_columns=list(unmatched_columns_set),
                total_records=total_records,
                status="accepted" if len(unmatched_columns_set) == 0 else "partial"
            )
            db.add(file_log)
        else:
            file_log.matched_columns = list(new_mapped.keys())
            temp_unmatched_columns = file_log.unmatched_columns
            for col in header:
                if col not in file_log.matched_columns:
                    temp_unmatched_columns.append(col)
            file_log.unmatched_columns = list(set(temp_unmatched_columns))
            file_log.total_records = total_records
            file_log.status = "accepted" if len(list(new_mapped.keys())) else "partial"

        db.commit()

        return {"status": "success", "message": "Data saved to Player, Batting, Bowling, and logs successfully"}

    except HTTPException as he:
        print(f"HTTPException: {str(he.detail)}")
        raise he
    except Exception as e:
        db.rollback()
        print(f"Error processing request: {e}")
        raise HTTPException(status_code=500, detail=f"Error processing request: {str(e)}")


def safe_int(value):
    """
    Safely convert a value to an integer.

    Attempts to convert the input value to an integer. If the conversion fails due
    to a `ValueError` or `TypeError`, it returns 0 as a fallback.

    Parameters:
        value (Any): The value to convert to an integer.

    Returns:
        int: Integer value if conversion is successful, otherwise 0.
    """
    
    try:
        return int(value)
    except (ValueError, TypeError):
        return 0

def safe_float(value):
    """
    Safely convert a value to a float.

    Attempts to convert the input value to a float. If the conversion fails due
    to a `ValueError` or `TypeError`, it returns 0.0 as a fallback.

    Parameters:
        value (Any): The value to convert to a float.

    Returns:
        float: Float value if conversion is successful, otherwise 0.0.
    """

    try:
        return float(value)
    except (ValueError, TypeError):
        return 0.0

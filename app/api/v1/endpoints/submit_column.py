from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from model.submit_column import ColumnMappingRequest
from db.session import get_db 
from services.save_data import save_submit_columns_data

router = APIRouter()



@router.post("/submit-columns")
async def submit_columns(request: ColumnMappingRequest, db: Session = Depends(get_db)):
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
        response = await save_submit_columns_data(request, db)
        return response
    except HTTPException as he:
        print(f"HTTPException: {str(he.detail)}")
        raise he
    except Exception as e:
        db.rollback()
        print(f"Error processing request: {e}")
        raise HTTPException(status_code=500, detail=f"Error processing request: {str(e)}")



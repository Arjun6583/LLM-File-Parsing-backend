# # api/v1/endpoints/fetch_data.py (same file or another)
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from db.session import get_db
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from services.fetch_data import get_file_metadata, get_mapped_data

router = APIRouter()
@router.get("/all-file-metadata")
def get_all_file_metadata(db: Session = Depends(get_db)):
    """
    Fetches metadata for all files that have user-mapped data.
    This endpoint retrieves a list of files that have been processed and mapped to user data,
    including their metadata such as matched columns, unmatched columns, total records, and status.
    Args:
        db (Session): SQLAlchemy session dependency for database operations.
    Returns:
        List[Dict]: A list of dictionaries containing file metadata, including:
            file_id (int): Unique ID of the file.
            file_name (str): Name of the file.
            file_type (str): Type/extension of the file.
            uploaded_date (datetime): Date when the file was uploaded.
            matched_columns (list): Columns successfully mapped to user data.
            unmatched_columns (list): Columns that could not be matched.
            total_records (int): Total number of records in the file.
            status (str): Processing status of the file.
            missing_columns (list): Columns expected in the schema but missing in the file.
    Raises:
        HTTPException: If an error occurs while fetching file metadata, a 500 error is returned with details.
    """
    try:
        
        result = get_file_metadata(db)
        print("Fetched file metadata:", result)
        return result
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=f"Error fetching file metadata: {str(e)}")

@router.get("/user-mapped-data/{file_id}")
def get_user_mapped_data(file_id: int, db: Session = Depends(get_db)):
    """
    Fetches file-mapped data for a specific file ID.
    This endpoint retrieves player statistics including batting and bowling details
    for a given file ID. It returns a list of players with their respective statistics.
    Args:
        file_id (int): Unique ID of the file for which user-mapped data is requested.
        db (Session): SQLAlchemy session dependency for database operations.
    Returns:
        List[List]: A list of lists where each inner list contains player statistics:
            player_name (str): Name of the player.
            matches (int): Number of matches played.
            innings (int): Number of innings played.
            total_runs (int): Total runs scored by the player.
            highest_score (int): Highest score achieved by the player.
            average (float): Batting average of the player.
            wickets (int): Number of wickets taken by the player.
            century (bool): Whether the player has scored a century.
            half_century (bool): Whether the player has scored a half-century.
            five_wickets (bool): Whether the player has taken five wickets in an innings.
            best_bowling (str): Best bowling performance of the player.
    Raises:
        HTTPException: If an error occurs while fetching file-mapped data, a 500 error is returned with details.
        
    """
    try:
        result = get_mapped_data(file_id, db)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching file mapped data: {str(e)}")

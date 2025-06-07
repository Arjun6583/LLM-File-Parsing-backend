# # api/v1/endpoints/fetch_data.py (same file or another)
from db.file_model import FileProcessingLog, FileUpload, Player, Batting, Bowling
from sqlalchemy.orm import Session
from fastapi import HTTPException
from sqlalchemy.orm import Session

def get_file_metadata(db: Session):
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
        user_mapped_file_ids = db.query(Player.file_id).distinct().all()
        user_mapped_file_ids = [fid[0] for fid in user_mapped_file_ids]

        files = db.query(FileUpload).filter(FileUpload.id.in_(user_mapped_file_ids)).all()
        result = []

        for file in files:
            metadata = db.query(FileProcessingLog).filter(FileProcessingLog.file_id == file.id).first()
            result.append({
                "file_id": file.id,
                "file_name": file.file_name,
                "file_type": file.file_type,
                "uploaded_date": file.uploaded_date,
                "matched_columns": metadata.matched_columns if metadata else [],
                "unmatched_columns": metadata.unmatched_columns if metadata else [],
                "total_records": metadata.total_records if metadata else 0,
                "status": metadata.status if metadata else "Unknown",
                "missing_columns": metadata.missing_columns if metadata else [],
            })
        print(len(result), " files found with user mapped data")
        files = db.query(FileUpload).all()
        for file in files:
            metadata = db.query(FileProcessingLog).filter(FileProcessingLog.file_id == file.id and FileProcessingLog.status == "rejected").first()
            result.append({
                "file_id": file.id,
                "file_name": file.file_name,
                "file_type": file.file_type,
                "uploaded_date": file.uploaded_date,
                "matched_columns": metadata.matched_columns if metadata else [],
                "unmatched_columns": metadata.unmatched_columns if metadata else [],
                "total_records": metadata.total_records if metadata else 0,
                "status": metadata.status if metadata else "Unknown",
                "missing_columns": metadata.missing_columns if metadata else [],
            })
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching file metadata: {str(e)}")

def get_mapped_data(file_id: int, db: Session):
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
        players = (
            db.query(Player)
            .filter(Player.file_id == file_id)
            .outerjoin(Batting)
            .outerjoin(Bowling)
            .all()
        )

        if not players:
            raise HTTPException(status_code=404, detail="No players found for given file ID")

        result = []
        for player in players:
            batting = player.batting
            bowling = player.bowling
            result.append([
                player.player_name,
                player.matches,
                player.innings,
                batting.total_runs if batting else None,
                batting.highest_score if batting else None,
                batting.average if batting else None,
                bowling.wickets if bowling else None,
                batting.century if batting else None,
                batting.half_century if batting else None,
                bowling.five_wickets if bowling else None,
                bowling.best_bowling if bowling else None
            ])

        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching file mapped data: {str(e)}")

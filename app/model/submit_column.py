from pydantic import BaseModel
from typing import List

class ColumnMappingRequest(BaseModel):
    file_id: int
    user_mapping: dict
    file_path: str
    file_type: str
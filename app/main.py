import fastapi
from fastapi.middleware.cors import CORSMiddleware
from api.v1.endpoints import upload_file, fetch_data, submit_column

app = fastapi.FastAPI(title="File Parsing")

origins = [
    "http://localhost:5173",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Disposition"]
)

app.include_router(upload_file.router, prefix="/upload", tags=["Upload"])
app.include_router(fetch_data.router, prefix="/fetch", tags=["Fetch"])
app.include_router(submit_column.router, prefix="/submit", tags=["Submit"])
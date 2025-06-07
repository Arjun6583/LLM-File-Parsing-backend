from dotenv import load_dotenv
import os
import json

load_dotenv()

FIREWORKS_API_KEY = os.getenv("FIREWORKS_API_KEY")
DB_URL = os.getenv("DB_URL")
MODEL = os.getenv("MODEL")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_MODEL = os.getenv("GROQ_MODEL")

GEMENI_API_KEY = os.getenv("GEMINI_API_KEY")
GEMENI_MODEL = os.getenv("GEMINI_MODEL")

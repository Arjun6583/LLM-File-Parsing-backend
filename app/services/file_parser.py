import pandas as pd
from docx import Document
import pdfplumber
import re


SCHEMA_MAPPING = {
    "Player Name": ["Player Name", "p_name", "player", "name", "full_name", "player_name", "player_full_name", "player_full"],
    "Matches": ["Matches", "mat", "match_count", "total_matches", "games", "total_games", "matches_played", "matches_count"],
    "Innings": ["Innings", "inns", "innings_count", "played_innings", "total_innings", "innings_played"],
    "Total Runs": ["Total Runs", "runs", "total_score", "r_total", "total_runs", "runs_scored", "total_runs_scored"],
    "Highest Score": ["Highest Score", "hs", "max_score", "top_score", "highest", "highest_score"],
    "Average": ["Average", "ave", "bat_avg", "average_runs", "avg", "batting_average"],
    "Wickets": ["Wickets", "wkt", "wickets_taken", "total_wickets", "wicket_count"],
    "Centuries": ["Centuries", "100s", "centuries", "century", "100"],
    "Half Centuries": ["Half Centuries", "50s", "half_centuries", "50", "half_century", "fifty"],
    "Fifers": ["Fifers", "five_wkts", "5_wickets", "5", "fifer", "five_wicket_haul"],
    "Best Bowling Figures": ["Best Bowling Figures", "bbf", "best_figures", "best_bowling", "best_bowling_figures", "best_bowling_figure"]
}

def extract_key_value_from_pdf(file_path):
    """
    Extracts key-value pairs from a PDF file where data is structured using 'key: value' format.

    This function processes each page of the PDF, extracting lines with ':' separators.
    When duplicate keys are encountered (indicating new player blocks), a new record is started.

    Args:
        file_path (str): Path to the input PDF file.

    Returns:
        pd.DataFrame: A DataFrame containing extracted records, one row per player or entry.

    Example:
        PDF Content:
        Name: Virat Kohli
        Matches: 111
        Runs: 8676

        Output:
        | Name         | Matches | Runs |
        |--------------|---------|------|
        | Virat Kohli  | 111     | 8676 |
    
    Exceptions:
        Returns an empty DataFrame if parsing fails or the structure is invalid.
    """

    try:
        players = []
        player_data = {}

        with pdfplumber.open(file_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                lines = text.split('\n')
                duplicate_keys = set()
                for line in lines:
                    if ':' in line:
                        key, value = map(str.strip, line.split(':', 1))
                        if key and value and key in duplicate_keys:
                            duplicate_keys = set()
                            players.append(player_data)
                            player_data = {}
                        if key and value:
                            player_data[key] = value
                            duplicate_keys.add(key)

        if player_data:
            players.append(player_data)

        return pd.DataFrame(players)
    except Exception as e:
        print(f"Error extracting key-values from PDF: {e}")
        return pd.DataFrame()

def extract_key_value_from_docx(file_path):
    """
    Extracts key-value pairs from a DOCX file where data follows the 'key: value' format.

    Parses each paragraph in the Word document. When a duplicate key is detected,
    it assumes the start of a new player block and adds the previous block to the list.

    Args:
        file_path (str): Path to the input DOCX file.

    Returns:
        pd.DataFrame: A DataFrame with one row per player or data block extracted.

    Example:
        DOCX Content:
        Name: Joe Root
        Matches: 130
        Runs: 11000

        Output:
        | Name     | Matches | Runs  |
        |----------|---------|-------|
        | Joe Root | 130     | 11000 |

    Exceptions:
        Returns an empty DataFrame if parsing fails or structure is inconsistent.
    """
    try:
        doc = Document(file_path)
        player_data = {}
        players = []
        duplicate_keys = set()
        for para in doc.paragraphs:
            text = para.text.strip()
            if not text:
                continue
            
            if ':' in text:
                key, value = map(str.strip, text.split(':', 1))
                if key and value and key in duplicate_keys:
                    duplicate_keys = set()
                    players.append(player_data)
                    player_data = {}
                if key and value:
                    player_data[key] = value
                    duplicate_keys.add(key)

        if player_data:
            players.append(player_data)

        return pd.DataFrame(players)
    except Exception as e:
        print(f"Error extracting key-values from DOCX: {e}")
        return pd.DataFrame()

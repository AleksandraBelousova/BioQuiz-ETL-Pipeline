# ETL Pipeline Project

## Overview
This project implements an automated ETL pipeline in Python, extracting data from `test.json`, transforming it (filtering, normalization, aggregation), and loading it into a SQLite database.

## Setup
1. Clone the repository: `git clone <repo-url>`
2. Create a virtual environment: `python -m venv venv`
3. Activate it: `source venv/bin/activate` (Unix) or `venv\Scripts\activate` (Windows)
4. Install dependencies: `pip install -r requirements.txt`

## Running the Script
- Manually: `python etl_script.py`
- Verify: Check `etl_data.db` and logs in the console.

## Automation
- Make executable: `chmod +x etl_script.py`
- Add to cron: `0 2 * * * /path/to/etl_script.py` (runs daily at 2:00 AM)

## Data Description
- **Source:** `test.json`
- **Structure:** Questions with fields `centerpiece` (text), `options` (list), `correct_options_idx` (list of indices).
- **Size:** 47 records.

## Transformations
- **Filter:** Questions with correct answer at index 0.
- **Normalize:** Lowercase question text.
- **Aggregate:** Count questions (default topic: 'biology').

## Database
- **Schema:** Two tables - `questions` (id, question_text, topic) and `options` (id, question_id, option_text, is_correct).
- **File:** `etl_data.db`
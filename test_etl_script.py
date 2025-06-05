import pytest
import pandas as pd
from etl_script import extract_data, transform_data, load_data
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

def test_extract_data(tmp_path):
    json_data = '{"centerpiece": "Test question", "options": ["A", "B"], "correct_options_idx": [0]}\n'
    file = tmp_path / "test.json"
    file.write_text(json_data)
    df = extract_data(str(file))
    assert len(df) == 1
    assert df["centerpiece"].iloc[0] == "Test question"

def test_transform_data():
    data = pd.DataFrame({"centerpiece": ["Test question"], "options": [["A", "B"]], "correct_options_idx": [[0]]})
    filtered_df, agg_df = transform_data(data)
    assert len(filtered_df) == 1
    assert filtered_df["centerpiece"].iloc[0] == "test question"
    assert agg_df["question_count"].iloc[0] == 1

def test_load_data(tmp_path):
    db_path = f"sqlite:///{tmp_path}/test.db"
    data = pd.DataFrame({"centerpiece": ["Test question"], "options": [["A", "B"]], "correct_options_idx": [[0]]})
    load_data(data, db_path)
    engine = create_engine(db_path)
    Session = sessionmaker(bind=engine)
    session = Session()
    assert session.query(Question).count() == 1
    assert session.query(Option).count() == 2
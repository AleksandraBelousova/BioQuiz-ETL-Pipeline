import argparse
import json
import logging
import pandas as pd
from sqlalchemy import Column, Integer, String, Boolean, ForeignKey, Index, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
Base = declarative_base()

class Question(Base):
    __tablename__ = 'questions'
    id = Column(Integer, primary_key=True)
    question_text = Column(String, nullable=False, unique=True)
    topic = Column(String, default='biology')
    __table_args__ = (Index('idx_question_text', 'question_text'),)

class Option(Base):
    __tablename__ = 'options'
    id = Column(Integer, primary_key=True)
    question_id = Column(Integer, ForeignKey('questions.id'), nullable=False)
    option_text = Column(String, nullable=False)
    is_correct = Column(Boolean, nullable=False)
    __table_args__ = (Index('idx_question_id', 'question_id'),)

def extract_data(file_path: str) -> pd.DataFrame:
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = [json.loads(line) for line in f if line.strip()
                    if all(k in json.loads(line) for k in {'centerpiece', 'options', 'correct_options_idx'})]
        if not data:
            raise ValueError(f"No valid records in {file_path}")
        logging.info(f"Extracted {len(data)} records from {file_path}")
        return pd.json_normalize(data)
    except FileNotFoundError:
        logging.error(f"File not found: {file_path}")
        raise
    except json.JSONDecodeError:
        logging.error(f"Invalid JSON in {file_path}")
        raise

def transform_data(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    try:
        df_filtered = df[df['correct_options_idx'].apply(lambda x: bool(x))].assign(
            centerpiece=lambda x: x['centerpiece'].str.lower().str.strip())
        agg_df = (df_filtered.groupby('topic', observed=True).size().reset_index(name='question_count')
                  if 'topic' in df_filtered.columns
                  else pd.DataFrame({'topic': ['biology'], 'question_count': [len(df_filtered)]}))
        logging.info(f"Transformed {len(df_filtered)} records")
        return df_filtered, agg_df
    except KeyError as e:
        logging.error(f"Missing column in DataFrame: {e}")
        raise

def load_data(df: pd.DataFrame, db_path: str = 'sqlite:///etl_data.db') -> None:
    try:
        engine = create_engine(db_path)
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        with Session() as session:
            questions = [
                Question(question_text=row['centerpiece'], topic=row.get('topic', 'biology'))
                for _, row in df.iterrows()
                if not session.query(Question).filter_by(question_text=row['centerpiece']).first()
            ]
            session.add_all(questions)
            session.flush()
            
            df_dict = {row['centerpiece']: row for _, row in df.iterrows()}
            options = [
                Option(
                    question_id=q.id,
                    option_text=opt,
                    is_correct=(idx in df_dict[q.question_text]['correct_options_idx'])
                )
                for q in questions
                for idx, opt in enumerate(df_dict[q.question_text]['options'])
                if isinstance(opt, str)
            ]
            session.add_all(options)
            session.commit()
            logging.info(f"Loaded {len(questions)} questions, {len(options)} options into {db_path}")
    except Exception as e:
        logging.error(f"Loading failed: {e}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL pipeline")
    parser.add_argument('--test_file', type=str, required=True, help='Test JSON file')
    parser.add_argument('--dev_file', type=str, required=True, help='Dev JSON file')
    parser.add_argument('--db_path', type=str, default='sqlite:///etl_data.db', help='DB path')
    args = parser.parse_args()
    
    for file in [args.test_file, args.dev_file]:
        df = extract_data(file)
        filtered_df, agg_df = transform_data(df)
        load_data(filtered_df, args.db_path)
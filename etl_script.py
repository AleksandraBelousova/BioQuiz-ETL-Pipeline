import json
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Boolean, ForeignKey
from sqlalchemy.orm import declarative_base
Base = declarative_base()
from sqlalchemy.orm import sessionmaker
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

Base = declarative_base()

class Question(Base):
    __tablename__ = 'questions'
    id = Column(Integer, primary_key=True)
    question_text = Column(String, nullable=False)
    topic = Column(String, default='biology')  
class Option(Base):
    __tablename__ = 'options'
    id = Column(Integer, primary_key=True)
    question_id = Column(Integer, ForeignKey('questions.id'), nullable=False)
    option_text = Column(String, nullable=False)
    is_correct = Column(Boolean, nullable=False)

def extract_data(file_path: str) -> pd.DataFrame:
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            # Read each line, parse as JSON, and collect into a list
            data = [json.loads(line) for line in f if line.strip()]
        # Convert list of dicts to DataFrame
        df = pd.json_normalize(data)
        logging.info(f"Extracted {len(df)} records from {file_path}")
        return df
    except Exception as e:
        logging.error(f"Extraction failed for {file_path}: {e}")
        raise

def transform_data(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    try:
        df_filtered = df[df['correct_options_idx'].apply(lambda x: x[0] == 0 if x else False)].copy()
        df_filtered['centerpiece'] = df_filtered['centerpiece'].str.lower().str.strip()
        
        if 'topic' in df_filtered.columns:
            agg_df = df_filtered.groupby('topic', observed=True).size().reset_index(name='question_count')
        else:
            agg_df = pd.DataFrame({'topic': ['biology'], 'question_count': [len(df_filtered)]})
        
        logging.info(f"Transformed data: {len(df_filtered)} records filtered")
        return df_filtered, agg_df
    except Exception as e:
        logging.error(f"Transformation failed: {e}")
        raise

def load_data(df: pd.DataFrame, db_path: str = 'sqlite:///etl_data.db') -> None:
    try:
        engine = create_engine(db_path, echo=False)
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        for _, row in df.iterrows():
            question = Question(question_text=row['centerpiece'], topic='biology') 
            session.add(question)
            session.flush() 
            
            for idx, opt in enumerate(row['options']):
                option = Option(
                    question_id=question.id,
                    option_text=opt,
                    is_correct=(idx in row['correct_options_idx'])
                )
                session.add(option)
        
        session.commit()
        logging.info(f"Loaded {len(df)} questions into {db_path}")
    except Exception as e:
        logging.error(f"Loading failed: {e}")
        session.rollback()
        raise
    finally:
        session.close()

if __name__ == "__main__":
    TEST_JSON_PATH = r"C:\Users\User-pc\Desktop\python\NewPortfolio2\test.json"
    DEV_JSON_PATH = r"C:\Users\User-pc\Desktop\python\NewPortfolio2\dev.json"
    DB_PATH = 'sqlite:///etl_data.db'
    
    test_df = extract_data(TEST_JSON_PATH)
    dev_df = extract_data(DEV_JSON_PATH)
    
    filtered_test, agg_test = transform_data(test_df)
    filtered_dev, agg_dev = transform_data(dev_df)
    
    load_data(filtered_test, DB_PATH)
    load_data(filtered_dev, DB_PATH)
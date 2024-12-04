import os
import pytest
import subprocess
import sqlite3
import pandas as pd

PIPELINE_PATH = os.path.abspath("./project/pipeline.py")

OUTPUT_PATHS = [
    os.path.abspath("./data/conflicts.sqlite"),
    os.path.abspath("./data/deaths.sqlite")]

def run_pipeline():
    subprocess.run(["python3", PIPELINE_PATH], check=True)

@pytest.fixture
def setup_pipeline():
    run_pipeline()

def test_conflicts_table_exists():
    conn = sqlite3.connect(OUTPUT_PATHS[0])
    cursor = conn.cursor()

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='conflicts';")
    result = cursor.fetchone()

    conn.close()
    assert result is not None, "Table 'conflicts' does not exist in the conflicts database."

def test_deaths_table_exists():
    conn = sqlite3.connect(OUTPUT_PATHS[1])
    cursor = conn.cursor()

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='deaths';")
    result = cursor.fetchone()

    conn.close()
    assert result is not None, "Table 'deaths' does not exist in the deaths database."

def test_if_output_file_exist():
    for OUTPUT_FILE_PATH in OUTPUT_PATHS: 
        assert os.path.exists(OUTPUT_FILE_PATH),  f"The output file at path {OUTPUT_FILE_PATH} doesn't exist"  

def test_dataframe_not_empty():
    df_conflicts = pd.read_sql("SELECT * FROM conflicts", sqlite3.connect(OUTPUT_PATHS[0]))
    df_deaths = pd.read_sql("SELECT * FROM deaths", sqlite3.connect(OUTPUT_PATHS[1]))

    assert not df_conflicts.empty, "The DataFrame for conflicts is empty."
    assert not df_deaths.empty, "The DataFrame for deaths is empty."

if __name__ == "__main__":
    pytest.main(["-v", "--tb=short", __file__])

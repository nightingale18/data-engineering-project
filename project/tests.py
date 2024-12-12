import os
import pytest
import subprocess
import sqlite3
import pandas as pd

from pipeline import load, transform_d1, transform_d2, prevent_errors
from pandas.testing import assert_frame_equal

PIPELINE_PATH = os.path.abspath("./project/pipeline.py")

def run_pipeline():
    subprocess.run(["python3", PIPELINE_PATH], check=True)

@pytest.fixture
def setup_pipeline():
    run_pipeline()

def test_load_table():
    table_name = 'test_load'
    table_path = 'sqlite:///data/test_load.sqlite'
    data = pd.DataFrame([[1,2,3], [3,4,5]], columns=['a', 'b', 'c'])
    load(data, table_name, table_path)

    conn = sqlite3.connect('./data/test_load.sqlite')
    result = pd.read_sql_query("SELECT * FROM test_load", conn)
    conn.close()

    assert_frame_equal(result, data)

# def test_transform_d1():
    
#     transform_d1()
#     conn = sqlite3.connect(OUTPUT_PATHS[1])
#     cursor = conn.cursor()

#     cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='deaths';")
#     result = cursor.fetchone()

#     conn.close()
#     assert result is not None, "Table 'deaths' does not exist in the deaths database."

# def test_transform_d2():
#     conn = sqlite3.connect(OUTPUT_PATHS[1])
#     cursor = conn.cursor()

#     cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='deaths';")
#     result = cursor.fetchone()

#     conn.close()
#     assert result is not None, "Table 'deaths' does not exist in the deaths database."

def test_load_table_exists():
    table_name = 'test_load'
    table_path = 'sqlite:///data/test_load.sqlite'
    data = pd.DataFrame([[1,2,3], [3,4,5]], columns=['a', 'b', 'c'])
    load(data, table_name, table_path)

    output_path = './data/test_load.sqlite'
    assert os.path.exists(output_path),  f"The output file at path {output_path} doesn't exist"  


def test_load_dataframe_not_empty():
    table_name = 'test_load'
    table_path = 'sqlite:///data/test_load.sqlite'
    # data = pd.DataFrame([], columns=['a'])
    data = pd.DataFrame([[1,2,3], [3,4,5]], columns=['a', 'b', 'c'])
    load(data, table_name, table_path)

    df_conflicts = pd.read_sql("SELECT * FROM test_load", sqlite3.connect('./data/test_load.sqlite'))

    assert not df_conflicts.empty, "The DataFrame for conflicts is not empty."

def test_prevent_errors():
    data = pd.DataFrame([[None,None,3], [3,None,4]], columns=['a', 'b', 'c'])
    data = prevent_errors(data).reset_index(drop=True).astype(int)
    
    
    result = pd.DataFrame([[3, 4]], columns=['a', 'c']).reset_index(drop=True).astype(int)
    print(result)
    print(data)
    assert not data.empty, "The DataFrame is not empty."

    # remove rows or columns with empty cells
    # print(data.to_dict())
    assert data.to_dict() == result.to_dict(), "could not remove all empty columns and every empty rows cells"
    # assert result==data, "could not remove all empty columns and every empty rows cells"
    # drop dublicates
    data = pd.DataFrame([[None,None,3], [3,None,4], [3,None,4]], columns=['a', 'b', 'c'])
    data = prevent_errors(data).reset_index(drop=True).astype(int)
    
    assert data.to_dict() == result.to_dict(), "could not remove duplicates"

if __name__ == "__main__":
    pytest.main(["-v", "--tb=short", __file__])

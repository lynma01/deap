# %%
import duckdb
import polars as pl
import os

# %%
from deap.file_utils import *

# %%
db_path = 'data/deap.db'
input_path = 'data/input/'
output_path = 'data/output/'

# %%
input_files = os.listdir(input_path)
output_files = os.listdir(output_path)

# %%
input_files
# %%
def confirm_deapdir() -> bool:
    """confirm current directory is 'deap'"""
    
    path_list = os.path.split(os.path.abspath('.'))
    
    if path_list[-1] == 'deap':
        return True
    else:
        return False
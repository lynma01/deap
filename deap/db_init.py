# %%
import duckdb
from deap.main import confirm_deapdir

# %%
def init_db() -> bool:
    if confirm_deapdir() and not os.path.exists(db_path):
        duckdb.connect(db_path)
        return True
    else:
        return False

# %%

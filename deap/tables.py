# %%
from dataclasses import dataclass
import polars as pl
# from deap.file_utils import dfile

# %%
@dataclass
class dtable:
#    source: dfile

    dataframe: pl.DataFrame
    columns: list

    def __init__(self, dataframe: pl.DataFrame):
        self.dataframe = dataframe
        self.columns = dataframe.columns
# %%

dframe = pl.read_csv('../data/input/black_sea_grain_initiative_voyages_data.csv')
dt = dtable(dataframe=dframe)
# %%

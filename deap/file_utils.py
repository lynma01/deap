# %%
import os
import polars as pl
from time import gmtime, strftime

def base_ingestion(file_path: str) -> pl.DataFrame:
    """create list list of files and extensions"""
    flist = []

    for files in os.listdir(file_path):
        fpaths = os.path.join(file_path, files)
        c_fpaths = clean_input_fnames(fpaths)
        spaths = split_fpath(c_fpaths)
        f_meta = ftime_conv(os.path.getctime(c_fpaths))
        fdata = [c_fpaths, os.path.basename(spaths[0]), spaths[-1], f_meta]

        parq_convr(fdata)

        flist.append(fdata)

    return flist_todf(flist)


def ftime_conv(fpath: os.PathLike) -> str:
    """convert epoch seconds to string"""
    return time.strftime("%Y-%m-%d_%H-%M-%S", time.gmtime(fpath))


def flist_todf(flist: list[list]) -> pl.DataFrame:
    """create dataframe of file lists with schema"""

    return pl.DataFrame(flist, schema=['path', 'name', 'ext', 'modified_dt'], orient='row')

def clean_str(fstr: str) -> str:
    """cleans and returns file_string"""
    return fstr.replace(' ', "_").replace('-', '').replace('__', '_').lower()


def clean_input_fnames(file_path: str) -> str:
    """clean all files names in the data/input dir"""
    
    ncase = clean_str(os.path.normpath(file_path))
    os.rename(file_path, ncase)
    return ncase


def split_fpath(file_path: str) -> tuple:
    """return a tuple of file path and extension"""
    return os.path.splitext(file_path)

def parq_convr(file_list: list):

    def write_pfile(df: pl.DataFrame, fpath: str, file_list: list):
        df.write_parquet(f"{fpath}{file_list[1]}.parquet")
    

    match file_list[-2]:
        case ['.csv']:
            print(files)
        case ['.xlsx']:
            print(files)
# %%
testing = base_ingestion("data/input/")
# %%


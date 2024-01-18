# %%
import os
import polars as pl
import datetime
import attrs

import pathlib

# %%
p = pathlib.Path("..")
[x for x in p.iterdir() if x.is_dir()]
# %%
@attrs.define
class DataFile:
    url_path: str
    basename: str = attrs.field(init=False) 
    file_extension: str = attrs.field(init=False)
    file_path: str = attrs.field(init=False)
    size: int = attrs.field(init=False)
    exists: bool = attrs.field(init=False)
    is_file: bool = attrs.field(init=False)
    is_directory: bool = attrs.field(init=False)
    # children: list = field(default_factory = list)
    # is_yaml: bool = False
    # cloud: dict = field(default_factory = dict)
    # cloud = {url: url, cloud_service: str, creds: str, cloud_path: str}

    @basename.default
    def _get_basename(self):
        return os.path.basename(self.url_path)
    
    @file_extension.default
    def _get_file_extension(self):
        return os.path.splitext(self.url_path)[-1]
    
    @file_path.default
    def _get_file_path(self):
        return os.path.ab
    
    @exists.default
    def _get_file_exists(self):
        return os.path.exists(self.url_path)

    def __init__(self, file_path: str):
        self.file_path = file_path
        self.file_extension = os.path.basename(file_path)
        self.size = f"{os.path.getsize(file_path) / 1000000} mb"
        self.is_directory = os.path.isdir(file_path)
        self.file_extension = os.path.splitext(file_path)[-1]
        self.parent_dir = os.path.dirname(file_path)
        self.creation_time = datetime.datetime.fromtimestamp(os.path.getctime(file_path)).strftime("%Y %m %d %H:%M:%S")
        self.exists = os.path.exists(file_path)


# %%
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


# %%


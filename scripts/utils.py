import pandas as pd
import pickle as pkl
import joblib
import json
import re
import multiprocessing
from typing import Any
from pathlib import PosixPath
from subprocess import Popen, PIPE, STDOUT

class REqual(str):
    "Override str.__eq__ to match a regex pattern."
    def __eq__(self, pattern):
        return re.fullmatch(pattern, self)


def save_obj(obj: Any, path: PosixPath, **kwargs) -> None:
    '''Save an object to a filepath'''
    suf = path.suffix[1:] # Filename Suffix
    if isinstance(obj, pd.DataFrame): # Pandas Dataframe
        kwargs['index'] = 0
        method = f"to_{suf}" if suf not in ("xlsx", "xls") else "to_excel"
        try:
            exec = getattr(pd, method)
            exec(path, **kwargs)
        except AttributeError as e:
            raise AttributeError(e)
    else:
        match REqual(suf):
            case "pkl|pickle": # Pickle File
                path.write_bytes(pkl.dumps(obj, **kwargs))
            case "joblib": # Joblib File
                joblib.dump(obj, path, *kwargs)
            case "json":
                path.write_bytes(json.dumps(obj, **kwargs))
            case _:
                pass

def load_obj(path: PosixPath, **kwargs) -> Any:
    '''Load and object from filepah'''
    suf = path.suffix[1:] # Filename Suffix
    match REqual(suf):
        case r"csv|parquet|xls|xlsx|orc|sql": # Pandas Dataframe
            method = f"read_{suf}" if suf not in ("xlsx", "xls") else "read_excel"
            try:
                exec = getattr(pd, method)
                return exec(path, **kwargs)
            except AttributeError as e:
                raise AttributeError(e)
        case "pkl|pickle": # Pickle File
            return pkl.loads(path.read_bytes(path), **kwargs)
        case "joblib": # Joblib File
            return joblib.load(path, **kwargs)
        case "json":
            return json.loads(path.read_bytes(path), **kwargs)
        case _:
            pass

def run_api(cmd: str) -> None:
    '''Run Command in a Jupyter Notebook Cell'''
    def call():
        p = Popen(
            cmd,
            stderr=STDOUT,
            stdout=PIPE,
            shell=True
        )
        with p.stdout:
            for line in iter(p.stdout.readline, b''):
                print(line.decode())
    multiprocessing.Process(target=call).start()
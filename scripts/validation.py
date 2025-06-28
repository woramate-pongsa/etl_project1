import os
import pandas as pd

def validation(file_path: str, **kwargs):
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"There is no file path: {file_path}")

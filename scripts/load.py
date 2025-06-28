import pandas as pd

def load(input_path: str, output_path: str,**kwargs):
    input_path.to_csv(output_path, index=False)
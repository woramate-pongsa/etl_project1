import pandas as pd

def transform(input_path: str, output_path: str, **kwargs):
    raw_data = pd.read_csv(input_path)
    raw_data['Date_time'] = pd.to_datetime(raw_data['Date_time'], format="%Y-%m-%d")
    raw_data['Month'] = raw_data['Date_time'].dt.month
    raw_data['Latitude'] = raw_data['Latitude'].round(2)
    raw_data['Longitude'] = raw_data['Longitude'].round(2)
    clean_data = raw_data.dropna()
    return clean_data
    

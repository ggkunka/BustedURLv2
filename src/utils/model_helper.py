import pandas as pd

def load_data_incrementally(filepath, chunk_size):
    for chunk in pd.read_csv(filepath, chunksize=chunk_size):
        yield chunk

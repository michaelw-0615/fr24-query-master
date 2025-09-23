import pandas as pd

def json_to_csv(in_path: str, out_path: str):
    df = pd.read_json(in_path)  
    df.to_csv(out_path, index=False)

if __name__ == "__main__":
    inpath = 'outputs/batch_flight_data.json'
    outpath = 'outputs/batch_flight_data.csv'
    json_to_csv(in_path=inpath, out_path=outpath)

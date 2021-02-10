import pandas as pd
from typing import Tuple

def read_data() -> pd.DataFrame:
    df = pd.read_json('starlink_historical_data.json')
    creation_date = pd.to_datetime(df.spaceTrack.apply(lambda x: x['CREATION_DATE']))
    df = pd.concat([df[['id', 'longitude', 'latitude']], creation_date], axis=1).set_index('spaceTrack').sort_index().rename(columns={'id': 'sat_id'})
    return df

"""
:sat_id: satellite id
:time: will return last known position before or at this time
Returns last known position as tuple of [lat, long]
"""
def get_last_position(df, sat_id: str, datetime: pd.Timestamp = pd.Timestamp.now()) -> Tuple[float, float]:
    out = df[(df.sat_id == sat_id) & (df.index <= datetime)].dropna()
    if len(out):
        return (out.iloc[-1].latitude, out.iloc[-1].longitude)
    else:
        return None


def get_closest_sat(df, coord: Tuple[float, float]) -> str:
    pass

def main():
    df = read_data()
    get_last_position(df, '5eefa85e6527ee0006dcee24', pd.Timestamp.now())
    



if __name__ == "__main__":
    main()


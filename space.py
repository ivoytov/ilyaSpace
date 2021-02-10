import pandas as pd
import sqlite3 as sq3
from typing import Tuple
from haversine import haversine_vector


def read_data() -> pd.DataFrame:
    """
    Reads data and stores in the SQL database.
    Any rows where either the Long or the Lat is Null will be omitted
    """
    df = pd.read_json("starlink_historical_data.json")
    creation_date = pd.to_datetime(df.spaceTrack.apply(lambda x: x["CREATION_DATE"]))
    df = (
        pd.concat([df[["id", "longitude", "latitude"]], creation_date], axis=1)
        .set_index("spaceTrack")
        .sort_index()
        .rename(columns={"id": "sat_id"})
        .dropna()
    )
    con = sq3.connect("space.db")
    df.to_sql("space", con=con, if_exists="replace")
    return con


def get_last_position(
    con, sat_id: str, datetime: pd.Timestamp = pd.Timestamp.now()
) -> Tuple[float, float]:
    """
    :con: SQL connection
    :sat_id: satellite id
    :time: will return last known position before or at this time
    Returns last known position as tuple of [lat, long]
    """
    df = pd.read_sql(
        "SELECT * FROM space", con=con, index_col="spaceTrack", parse_dates="spaceTrack"
    )
    out = df[(df.sat_id == sat_id) & (df.index <= datetime)].dropna()
    if len(out):
        return (out.iloc[-1].latitude, out.iloc[-1].longitude)
    else:
        return None


def get_closest_sat(
    con, coord: Tuple[float, float], datetime: pd.Timestamp = pd.Timestamp.now()
) -> Tuple[str, float]:
    df = pd.read_sql(
        "SELECT * FROM space", con=con, index_col="spaceTrack", parse_dates="spaceTrack"
    )
    """
    :con: SQL connection
    :coord: coordinates in (lat,long) format
    :datetime: time when you are looking for the satellite
    returns None if no satellites found or id of the sat
    """
    if datetime not in df.index:
        raise KeyError("No information available for specified datetime")

    sats = df.loc[datetime].set_index("sat_id")
    if not len(sats):
        return None

    sats = sats[["latitude", "longitude"]]  # reorder cols
    dists = pd.Series(
        haversine_vector([coord] * len(sats), sats.values), index=sats.index
    )
    return dists.idxmin(), dists.min()


def main():
    df = read_data()
    get_last_position(df, "5eefa85e6527ee0006dcee24", pd.Timestamp.now())


if __name__ == "__main__":
    main()

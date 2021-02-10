import pandas as pd
from contextlib import closing
import sqlite3 as sq3
from typing import Tuple
from haversine import haversine_vector
import numpy as np


def read_data(con) -> None:
    """
    Reads data and stores in the SQL database.
    Any rows where either the Long or the Lat is Null will be omitted
    :con: SQL connection
    """
    df = pd.read_json("starlink_historical_data.json")
    creation_date = (
        pd.to_datetime(df.spaceTrack.apply(lambda x: x["CREATION_DATE"])).astype(
            np.int64
        )
        // 10 ** 9
    )
    df = (
        pd.concat([df[["id", "longitude", "latitude"]], creation_date], axis=1)
        .set_index("spaceTrack")
        .sort_index()
        .rename(columns={"id": "sat_id"})
        .dropna()
    )
    df.to_sql("space", con=con, if_exists="replace")


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
        f"SELECT * from space WHERE sat_id == '{sat_id}' and spaceTrack <= {datetime.timestamp()} ORDER BY spaceTrack DESC LIMIT 1",
        con=con,
        index_col="spaceTrack",
        parse_dates="spaceTrack",
    )
    out = df[(df.sat_id == sat_id) & (df.index <= datetime)]
    if len(out):
        return (out.iloc[-1].latitude, out.iloc[-1].longitude)
    else:
        return None


def get_closest_sat(
    con, coord: Tuple[float, float], datetime: pd.Timestamp = pd.Timestamp.now()
) -> Tuple[str, float]:
    """
    :con: SQL connection
    :coord: coordinates in (lat,long) format
    :datetime: time when you are looking for the satellite
    returns None if no satellites found or id of the sat
    """
    df = pd.read_sql(
        f"SELECT * FROM space WHERE spaceTrack == {datetime.timestamp()}",
        con=con,
        index_col="spaceTrack",
        parse_dates="spaceTrack",
    )
    sats = df.loc[datetime].set_index("sat_id")

    if not len(sats):
        raise KeyError("No information available for specified datetime")

    sats = sats[["latitude", "longitude"]]  # reorder cols
    dists = pd.Series(
        haversine_vector([coord] * len(sats), sats.values), index=sats.index
    )
    return dists.idxmin(), dists.min()


def _get_date() -> pd.Timestamp:
    datetime = input("What date do you want to use ([Enter] for now):")
    datetime = pd.Timestamp.now() if datetime == "" else datetime
    return pd.Timestamp(datetime)


def main():
    with closing(sq3.connect("space.db")) as con:
        while True:
            val = input(
                "Enter 1 to get last position, 2 to get closest sat, 3 to read data, [Enter] to exit"
            )
            if val == "1":
                sat_id = input("Enter satetllite id:")
                if len(sat_id) != 24:
                    print("Satellite ids must be 24 chars long")
                    continue
                try:
                    datetime = _get_date()
                except:
                    print("Cannot read datetime, try 2020-05-19 06:26:10 format")
                    continue
                print("Last position", get_last_position(con, sat_id, datetime))

            elif val == "2":
                try:
                    lat = float(input("Enter latitude:"))
                    lon = float(input("Enter longitude:"))
                except:
                    print("Bad input")
                    continue
                try:
                    datetime = _get_date()
                except:
                    print("Cannot read datetime, try 2020-05-19 06:26:10 format")
                    continue

                sat_id, dist = get_closest_sat(con, (lat, lon), datetime)
                print("Closest satellite id", sat_id, "distance (kms)", round(dist, 2))
            elif val == "3":
                read_data(con)
            else:
                break


if __name__ == "__main__":
    main()

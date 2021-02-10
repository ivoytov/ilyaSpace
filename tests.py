import unittest
import pandas as pd
from contextlib import closing

from space import *

class SpaceTest(unittest.TestCase):
    def setUp(self):
        self.con = sq3.connect("space.db")
        read_data(self.con)

    def test_data_import(self):
        df = pd.read_sql(
            "SELECT * FROM space", con=self.con, index_col="spaceTrack", parse_dates="spaceTrack"
        )
        self.assertEqual(df.shape, (3027, 3))

    def test_bad_sat_id(self):
        out = get_last_position(self.con, 'UFO')
        self.assertIsNone(out)

    def test_sat_never_appears(self):
        out = get_last_position(self.con, '5eefa85e6527ee0006dcee24', pd.Timestamp.now())
        self.assertIsNone(out)

    def test_time_exactly(self):
        # time is exactly the time of an appearance
        out = get_last_position(self.con, '5f36cb59bd88830006274090', pd.Timestamp('2021-01-26 06:26:10'))
        self.assertEqual(out, (-42.09235170986195, 94))

    def test_time_too_eary(self):
        # time is one second before first appearance
        out = get_last_position(self.con, '5f36cb59bd8883000627409e', pd.Timestamp('2021-01-21 06:26:09'))
        self.assertIsNone(out)


    def test_dist_reg(self):
        # almost exact location
        coord = (-42.7, 67.0)
        sat_id, dist = get_closest_sat(self.con, coord, pd.Timestamp('2021-01-26 14:26:10'))
        self.assertEqual(sat_id, '5f889669c86e27000615b262')

    def test_bad_sat(self):
        # uses today's date, when no data is available
        coord = (-42.7, 67.0)
        self.assertRaises(KeyError, get_closest_sat, self.con, coord)

if __name__ == '__main__':
    unittest.main()

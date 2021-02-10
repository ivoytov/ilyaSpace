import unittest
import pandas as pd

from space import *

class SpaceTest(unittest.TestCase):
    def setUp(self):
        self.df = read_data()

    def test_data_import(self):
        self.assertEqual(self.df.shape, (3141,3))

    def test_bad_sat_id(self):
        out = get_last_position(self.df, 'UFO')
        self.assertIsNone(out)

    def test_sat_never_appears(self):
        out = get_last_position(self.df, '5eefa85e6527ee0006dcee24', pd.Timestamp.now())
        self.assertIsNone(out)

    def test_time_exactly(self):
        # time is exactly the time of an appearance
        out = get_last_position(self.df, '5f36cb59bd88830006274090', pd.Timestamp('2021-01-26 06:26:10'))
        self.assertEqual(out, (-42.09235170986195, 94))

    def test_time_too_eary(self):
        # time is one second before first appearance
        out = get_last_position(self.df, '5f36cb59bd8883000627409e', pd.Timestamp('2021-01-21 06:26:09'))
        self.assertIsNone(out)



if __name__ == '__main__':
    unittest.main()

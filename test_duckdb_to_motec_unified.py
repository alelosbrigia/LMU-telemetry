import unittest
import pandas as pd

from duckdb_to_motec_unified import compute_lap_channels


class LapDetectionTests(unittest.TestCase):
    def test_beacon_and_laptime_from_normalized_position(self):
        # Two lap wraps -> two beacon pulses
        time = pd.Series([0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0], name="Time")
        lap_pos = pd.Series([0.0, 0.5, 0.99, 0.02, 0.5, 0.98, 0.01], name="NormalizedLap")
        df = pd.DataFrame({"Time": time, "NormalizedLap": lap_pos})

        beacon, lap_time, lap, _ = compute_lap_channels(df)

        self.assertListEqual(beacon.tolist(), [1, 0, 0, 1, 0, 0, 1])
        self.assertListEqual(lap_time.round(6).tolist(), [0.0, 1.0, 2.0, 0.0, 1.0, 2.0, 0.0])
        self.assertListEqual(lap.tolist(), [1, 1, 1, 2, 2, 2, 3])

    def test_fallback_without_lap_signal(self):
        time = pd.Series([0.0, 0.5, 1.0], name="Time")
        df = pd.DataFrame({"Time": time, "Speed": [10, 20, 30]})

        beacon, lap_time, lap, _ = compute_lap_channels(df)

        self.assertListEqual(beacon.tolist(), [0, 0, 0])
        self.assertListEqual(lap_time.tolist(), time.tolist())
        self.assertListEqual(lap.tolist(), [1, 1, 1])

    def test_csv_beacon_is_integer_pulse(self):
        time = pd.Series([0.0, 1.0, 2.0, 3.0], name="Time")
        lap_pos = pd.Series([0.0, 0.99, 0.02, 0.5], name="NormalizedLap")
        df = pd.DataFrame({"Time": time, "NormalizedLap": lap_pos})

        beacon, lap_time, lap, _ = compute_lap_channels(df)
        out = pd.DataFrame({"Time": time, "Beacon": beacon, "LapTime": lap_time, "Lap": lap})
        csv = out[["Time", "Beacon", "LapTime"]].to_csv(index=False, float_format="%.6f").strip().splitlines()

        beacons = [line.split(",")[1] for line in csv[1:]]
        self.assertListEqual(beacons, ["1", "0", "1", "0"])


if __name__ == "__main__":
    unittest.main()

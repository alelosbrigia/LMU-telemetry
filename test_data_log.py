import unittest

from data_log import DataLog, Message


class DataLogResampleTests(unittest.TestCase):
    def test_resample_skips_beacon_channel(self):
        log = DataLog()
        log.add_channel("Beacon", "", int, 0, Message(0.0, 1))
        log.channels["Beacon"].messages.extend([Message(1.0, 0), Message(2.0, 1)])

        log.add_channel("Speed", "km/h", float, 2, Message(0.0, 10.0))
        log.channels["Speed"].messages.extend([Message(1.0, 20.0), Message(2.0, 30.0)])

        log.resample(1.0, skip_channels={"Beacon"})

        self.assertEqual([m.value for m in log.channels["Beacon"].messages], [1, 0, 1])
        self.assertEqual(len(log.channels["Speed"].messages), 2)  # resampled to duration=2s -> 2 samples


if __name__ == "__main__":
    unittest.main()

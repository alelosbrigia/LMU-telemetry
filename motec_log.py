import datetime
import struct
from concurrent.futures import ProcessPoolExecutor

import numpy as np

from data_log import Channel, DataLog, Message
from ldparser.ldparser import ldChan, ldData, ldEvent, ldHead, ldVehicle, ldVenue


class BeaconEventBlock:
    def __init__(self, times):
        self.times = list(times)

    def write(self, f):
        # Simple block: [uint32 count][float32 times...]
        f.write(struct.pack("<I", len(self.times)))
        if self.times:
            f.write(np.asarray(self.times, dtype=np.float32).tobytes())


def _prepare_channel_data(log_channel):
    """Convert a DataLog channel into a numpy array for ldparser.

    This helper is defined at module scope so it can be executed in a
    ``ProcessPoolExecutor`` to spread the conversion work across CPUs.
    """

    data_type = np.float32 if log_channel.data_type is float else np.int32
    data_array = np.fromiter(
        (msg.value for msg in log_channel.messages), dtype=data_type, count=len(log_channel.messages)
    )

    return {
        "data_array": data_array,
        "data_len": len(log_channel.messages),
        "data_type": data_type,
        "freq": int(log_channel.avg_frequency()),
    }

class MotecLog(object):
    """ Handles generating a MoTeC .ld file from log data.

    This utilizes the ldparser library for packing all the meta data and channel data into the
    correct binary format. Some functionality and information (e.g. pointer constants below) was
    missing from the ldparser library, so this class servers as a wrapper to fill in the gaps.

    This operates on containers from the data_log library.
    """
    # Pointers to locations in the file where data sections should be written. These have been
    # determined from inspecting some MoTeC .ld files, and were consistent across all files.
    VEHICLE_PTR = 1762
    VENUE_PTR = 5078
    EVENT_PTR = 8180
    HEADER_PTR = 11336

    CHANNEL_HEADER_SIZE = struct.calcsize(ldChan.fmt)

    def __init__(self):
        self.driver = ""
        self.vehicle_id = ""
        self.vehicle_weight = 0
        self.vehicle_type = ""
        self.vehicle_comment = ""
        self.venue_name = ""
        self.event_name = ""
        self.event_session = ""
        self.long_comment = ""
        self.short_comment = ""
        self.datetime = datetime.datetime.now()
        self.beacon_times = []

        # File components from ldparser
        self.ld_header = None
        self.ld_channels = []

    def initialize(self):
        """ Initializes all the meta data for the motec log.

        This must be called before adding any channel data.
        """
        ld_vehicle = ldVehicle(self.vehicle_id, self.vehicle_weight, self.vehicle_type, \
            self.vehicle_comment)
        ld_venue = ldVenue(self.venue_name, self.VEHICLE_PTR, ld_vehicle)
        ld_event = ldEvent(self.event_name, self.event_session, self.long_comment, \
            self.VENUE_PTR, ld_venue)

        self.ld_header = ldHead(self.HEADER_PTR, self.HEADER_PTR, 0, ld_event, \
            self.driver, self.vehicle_id, self.venue_name, self.datetime, self.short_comment, \
            self.event_name, self.event_session)

    def add_beacons(self, times):
        """Store beacon (lap marker) timestamps in seconds."""
        self.beacon_times = list(times or [])

    def add_channel(self, log_channel, prepared_data=None):
        """Adds a single channel of data to the motec log.

        Parameters
        ----------
        log_channel : data_log.Channel
            Channel to convert into ldparser structures.
        prepared_data : dict | None
            Optional pre-computed data produced by ``_prepare_channel_data``.
        """
        # Advance the header data pointer
        self.ld_header.data_ptr += self.CHANNEL_HEADER_SIZE

        # Advance the data pointers of all previous channels
        for ld_channel in self.ld_channels:
            ld_channel.data_ptr += self.CHANNEL_HEADER_SIZE

        # Determine our file pointers
        if self.ld_channels:
            meta_ptr = self.ld_channels[-1].next_meta_ptr
            prev_meta_ptr = self.ld_channels[-1].meta_ptr
            data_ptr = self.ld_channels[-1].data_ptr + self.ld_channels[-1]._data.nbytes
        else:
            # First channel needs the previous pointer zero'd out
            meta_ptr = self.HEADER_PTR
            prev_meta_ptr = 0
            data_ptr = self.ld_header.data_ptr
        next_meta_ptr = meta_ptr + self.CHANNEL_HEADER_SIZE

        # Channel specs
        data_len = prepared_data["data_len"] if prepared_data else len(log_channel.messages)
        data_type = prepared_data["data_type"] if prepared_data else (
            np.float32 if log_channel.data_type is float else np.int32
        )
        freq = prepared_data["freq"] if prepared_data else int(log_channel.avg_frequency())
        shift = 0
        multiplier = 1
        scale = 1

        # Decimal places must be hard coded to zero, the ldparser library doesn't properly
        # handle non zero values, consequently all channels will have zero decimal places
        # decimals = log_channel.decimals
        decimals = 0

        ld_channel = ldChan(None, meta_ptr, prev_meta_ptr, next_meta_ptr, data_ptr, data_len, \
            data_type, freq, shift, multiplier, scale, decimals, log_channel.name, "", \
            log_channel.units)

        # Add in the channel data
        if prepared_data and "data_array" in prepared_data:
            ld_channel._data = prepared_data["data_array"]
        else:
            ld_channel._data = np.fromiter(
                (msg.value for msg in log_channel.messages), dtype=data_type, count=data_len
            )

        # Add the ld channel and advance the file pointers
        self.ld_channels.append(ld_channel)

    def add_all_channels(self, data_log, max_workers=None):
        """Adds all channels from a DataLog to the motec log.

        Parameters
        ----------
        data_log : data_log.DataLog
            Log container holding the channel data to convert.
        max_workers : int | None
            Optional override for the number of worker processes used when
            converting channel payloads to numpy arrays. ``None`` lets
            ``ProcessPoolExecutor`` decide based on CPU count. Use ``1`` to
            force sequential execution.
        """

        channel_items = list(data_log.channels.items())
        prepared_channels = {}
        use_parallel = max_workers != 1 and len(channel_items) > 1

        if use_parallel:
            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                future_to_name = {
                    executor.submit(_prepare_channel_data, channel): name for name, channel in channel_items
                }

                for future in future_to_name:
                    name = future_to_name[future]
                    prepared_channels[name] = future.result()

        for channel_name, channel in channel_items:
            self.add_channel(channel, prepared_channels.get(channel_name))

    def write(self, filename):
        """ Writes the motec log data to disc. """
        # Check for the presence of any channels, since the ldData write() method doesn't
        # gracefully handle zero channels
        if self.ld_channels:
            # If we have beacon times, place the event block right after the last channel data
            if self.beacon_times and self.ld_channels:
                last_channel = self.ld_channels[-1]
                beacons_ptr = last_channel.data_ptr + last_channel._data.nbytes
                self.ld_header.aux_ptr = beacons_ptr
                self.ld_header.aux = BeaconEventBlock(self.beacon_times)

            ld_data = ldData(self.ld_header, self.ld_channels)

            # Need to zero out the final channel pointer
            ld_data.channs[-1].next_meta_ptr = 0

            ld_data.write(filename)
        else:
            with open(filename, "wb") as f:
                self.ld_header.write(f, 0)

from local_settings import SPEED_TEST_HOST, SPEED_TEST_PATH, BANDWIDTH_INTERVAL, FULL_BANDWIDTH_SIZES, FULL_BANDWIDTH_RATIO, BANDWIDTH_SIZES
from setproctitle import setproctitle
import datetime
import httplib
import json
import redis
import time
import socket


assert isinstance(SPEED_TEST_HOST, str)
assert isinstance(SPEED_TEST_PATH, str)
assert isinstance(BANDWIDTH_INTERVAL, (int, float))
assert BANDWIDTH_INTERVAL > 5
assert isinstance(FULL_BANDWIDTH_RATIO, int)
assert FULL_BANDWIDTH_RATIO > 0
assert isinstance(BANDWIDTH_SIZES, (tuple, list))
assert isinstance(FULL_BANDWIDTH_SIZES, (tuple, list))


class InternetConnectionSpeed(object):

    def __init__(self):
        self.redis = redis.StrictRedis()

    @classmethod
    def get_random_data(cls, size):
        f = open("/dev/urandom")
        remaining_size = size
        data = ""
        while remaining_size > 0:
            data += f.read(remaining_size)
            remaining_size = size - len(data)
        return data

    def measure_download(self, size):
        start = time.time()
        conn = httplib.HTTPConnection(SPEED_TEST_HOST, timeout=5)
        try:
            conn.connect()
            connected = time.time()
            conn.request('GET', "/{path}?size={size}".format(path=SPEED_TEST_PATH, size=size))
            request_time = time.time()
            resp = conn.getresponse()
            response_time = time.time()
            response_content = resp.read()
            size = len(response_content)
            transferred = time.time()
            conn.close()
        except (socket.gaierror, socket.timeout, socket.error) as err:
            print "Connecting to %s failed: %s" % (SPEED_TEST_HOST, err)
            return None

        gen_time = float(resp.getheader("X-gen-duration"))
        transfer_time = transferred - response_time - gen_time

        data = {
            "fields": {
                "connection_opened": round(connected - start, 8),
                "request_sent": round(request_time - connected, 8),
                "data_transfer": round(transfer_time, 8),
                "size_in_bytes": size,
                "server_generation": round(gen_time, 8),
                "speed_mbits": round((float(size) / (transfer_time) * 8 / 1024 / 1024), 4),
            },
            "tags": {
                "direction": "download",
                "size": size,
                "source": "http",
            },
            "measurement": "internet_speed",
            "time": datetime.datetime.utcnow().isoformat() + "Z",
        }

        return data

    def measure_upload(self, size):
        upload_data = self.get_random_data(size)
        size = len(upload_data)
        start = time.time()
        conn = httplib.HTTPConnection(SPEED_TEST_HOST, timeout=5)
        try:
            conn.connect()
            connected = time.time()
            conn.request('POST', "/" + SPEED_TEST_PATH, upload_data)
            request_time = time.time()
            resp = conn.getresponse()
            resp.read()
            transfer_time = time.time()
            conn.close()
        except (socket.gaierror, socket.timeout, socket.error) as err:
            print "Network error occurred: %s - %s" % (SPEED_TEST_HOST, err)
            return None

        data = {
            "fields": {
                "connection_opened": round(connected - start, 8),
                "data_transfer": round(request_time - connected, 8),
                "response_received": round(transfer_time - request_time, 8),
                "size_in_bytes": size,
                "speed_mbits": round((float(size) / (request_time - connected) * 8 / 1024 / 1024), 4),
            },
            "tags": {
                "direction": "upload",
                "source": "http",
                "size": size,
            },
            "measurement": "internet_speed",
            "time": datetime.datetime.utcnow().isoformat() + "Z",
        }
        return data

    def fetch_once(self, sizes):
        measurements = []
        for size in sizes:
            download_data = self.measure_download(size)
            if download_data:
                measurements.append(download_data)
            upload_data = self.measure_upload(size)
            if upload_data:
                measurements.append(upload_data)
        if len(measurements) > 0:
            self.redis.publish("influx-update-pubsub", json.dumps(measurements))

    def run(self):
        last_fetch_at = time.time()
        i = 0
        while True:
            i = i % FULL_BANDWIDTH_RATIO
            if i == 0:
                sizes = FULL_BANDWIDTH_SIZES
            else:
                sizes = BANDWIDTH_SIZES
            self.fetch_once(sizes)
            sleep_time = max(BANDWIDTH_INTERVAL / 4, BANDWIDTH_INTERVAL - (time.time() - last_fetch_at))
            last_fetch_at = time.time()
            print "Sleeping for %s" % sleep_time
            time.sleep(sleep_time)
            i += 1


def main():
    icp = InternetConnectionSpeed()
    icp.run()


if __name__ == '__main__':
    main()

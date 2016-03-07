from influxdb import InfluxDBClient
from local_settings import PING_DESTINATIONS, PING_INTERVAL, PING_COUNT
from setproctitle import setproctitle
import datetime
import httplib
import json
import redis
import subprocess
import time
import utils


assert isinstance(PING_DESTINATIONS, list)
assert isinstance(PING_INTERVAL, (int, float))
assert isinstance(PING_COUNT, (int))
assert PING_INTERVAL >= 5
assert PING_COUNT > 0


class DateTimeEncoder(json.JSONEncoder):

    def default(self, o):
        if isinstance(o, datetime.datetime):
            return o.isoformat()

        return json.JSONEncoder.default(self, o)


class PingSpeed(object):

    def __init__(self):
        self.redis = redis.StrictRedis()
        self.influx = InfluxDBClient("localhost", 8086, "root", "root", "home")

    @classmethod
    def parse_fping(cls, stdout, stderr):
        stderr = stderr.strip().split("\n")

        data = []

        for line in stderr:
            line = line.split(" : ")
            if len(line) != 2:
                continue
            hostname = line[0].strip()
            pings = line[1].strip()
            pings = pings.split(" ")
            filtered_pings = filter(lambda x: x != "-", pings)
            filtered_pings = map(float, filtered_pings)
            fields = {
                "pl": utils.calc_pl(filtered_pings, pings),
            }
            fields.update(utils.calc_stats(filtered_pings))
            content = {
                "hostname": hostname,
                "fields": fields,
            }
            data.append(content)
        return data

    def ping(self, config):
        destinations = [item[0] for item in config]
        hostname_map = dict(config)
        command = ["fping", "-C%s" % PING_COUNT, "-q"]
        command.extend(destinations)
        fping_output = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = fping_output.communicate()
        parsed_fping = self.parse_fping(stdout, stderr)
        output = []
        for item in parsed_fping:
            data = {
                "fields": item["fields"],
                "tags": {
                    "destination": item["hostname"],
                    "alias": hostname_map.get(item["hostname"]),
                },
                "measurement": "ping",
                "time": datetime.datetime.utcnow().isoformat() + "Z",
            }
            output.append(data)
        return output

    def fetch_once(self):
        measurements = self.ping(PING_DESTINATIONS)
        self.redis.publish("influx-update-pubsub", json.dumps(measurements, cls=DateTimeEncoder))
        self.influx.write_points(measurements)

    def run(self):
        last_fetch_at = time.time()
        while True:
            self.fetch_once()
            sleep_time = max(PING_INTERVAL / 4, PING_INTERVAL - (time.time() - last_fetch_at))
            last_fetch_at = time.time()
            print "Sleeping for %s" % sleep_time
            time.sleep(sleep_time)


def main():
    icp = PingSpeed()
    icp.run()


if __name__ == '__main__':
    main()

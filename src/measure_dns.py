from local_settings import NAMESERVERS, VALID_DESTINATIONS, INVALID_DESTINATIONS, VALID_RANDOM_DESTINATIONS, INVALID_RANDOM_DESTINATIONS, DNS_INTERVAL
import datetime
import dns.resolver
import json
import redis
import time
import uuid
import utils


assert isinstance(NAMESERVERS, (list, tuple))
assert isinstance(VALID_DESTINATIONS, (list, tuple))
assert isinstance(INVALID_DESTINATIONS, (list, tuple))
assert isinstance(VALID_RANDOM_DESTINATIONS, (list, tuple))
assert isinstance(INVALID_RANDOM_DESTINATIONS, (list, tuple))
assert isinstance(DNS_INTERVAL, (int, float))


def time_method(func):
    def inner(*args, **kwargs):
        start_time = time.time()
        func(*args, **kwargs)
        end_time = time.time()
        return round(float(end_time - start_time), 4)
    return inner


class DnsSpeed(object):

    def __init__(self):
        self.redis = redis.StrictRedis()
        self.resolver = dns.resolver.Resolver()
        self.resolver.timeout = 2
        self.resolver.lifetime = 2

    @time_method
    def resolve_dns(self, hostname):
        try:
            self.resolver.query(hostname, "A")
        except dns.resolver.NXDOMAIN:
            pass

    def test(self, hostname):
        response_times = []
        data = {}
        for a in range(1, 6):
            try:
                response_time = self.resolve_dns(hostname)
            except dns.exception.DNSException:
                response_time = None
            response_times.append(response_time)
            data["try-%s" % a] = response_time
        data.update(utils.calc_stats(filter(lambda x: x is not None, response_times)))
        return data

    def write_data(self, nameserver, destination, data, extra_tags=None):
        tags = {
            "nameserver": nameserver,
            "destination": destination,
        }
        if extra_tags is not None:
            tags.update(extra_tags)
        influx_data = {
            "measurement": "dns_speed",
            "tags": tags,
            "time": datetime.datetime.utcnow().isoformat() + "Z",
            "fields": data,
        }
        return influx_data

    def fetch_once(self):
        measurements = []
        for nameserver in NAMESERVERS:
            self.resolver.nameservers = [nameserver]
            for destination in VALID_DESTINATIONS:
                data = self.test(destination)
                measurements.append(self.write_data(nameserver, destination, data, {"type": "valid", "class": "predefined"}))
            for destination in INVALID_DESTINATIONS:
                data = self.test(destination)
                measurements.append(self.write_data(nameserver, destination, data, {"type": "invalid", "class": "predefined"}))
            for destination in VALID_RANDOM_DESTINATIONS:
                generated_destination = str(uuid.uuid4()) + "." + destination
                data = self.test(generated_destination)
                measurements.append(self.write_data(nameserver, destination, data, {"type": "valid", "class": "random"}))
            for destination in INVALID_RANDOM_DESTINATIONS:
                generated_destination = str(uuid.uuid4()) + "." + destination
                data = self.test(generated_destination)
                measurements.append(self.write_data(nameserver, destination, data, {"type": "invalid", "class": "random"}))
        self.redis.publish("influx-update-pubsub", json.dumps(measurements))

    def run(self):
        last_fetch_at = time.time()
        while True:
            self.fetch_once()
            sleep_time = max(DNS_INTERVAL / 4, DNS_INTERVAL - (time.time() - last_fetch_at))
            last_fetch_at = time.time()
            print(f"Sleeping for {sleep_time}s")
            time.sleep(sleep_time)


def main():
    ds = DnsSpeed()
    ds.run()


if __name__ == '__main__':
    main()

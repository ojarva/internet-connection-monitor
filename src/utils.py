import math


def calc_stats(items):
    return {
        "avg": average(items),
        "max": get_max(items),
        "min": get_min(items),
        "stddev": calc_stddev(items),
    }


def average(s):
    if len(s) == 0:
        return None
    return round(sum(s) * 1.0 / len(s), 4)


def calc_stddev(samples):
    if len(samples) == 0:
        return None
    avg = average(samples)
    variance = map(lambda x: (x - avg)**2, samples)
    return round(math.sqrt(average(variance)), 3)


def calc_pl(filtered_pings, pings):
    if len(pings) == 0:
        return None
    return round(float(1 - float(len(filtered_pings)) / len(pings)), 3)


def get_min(pings):
    if len(pings) == 0:
        return None
    return min(pings)


def get_max(pings):
    if len(pings) == 0:
        return None
    return max(pings)

import re
from datetime import timedelta


cre_runtime_limit = re.compile(r'(?P<value>\d+(\.\d+)?)(?P<scale>[mhd]?)')
runtime_scales = {'m': 1, 'h': 60, 'd': 60 * 24}


cre_memory_limit = re.compile(r'(?P<value>\d+(\.\d+)?)(?P<scale>[mMgG]?)')
memory_scales = {'m': 1, 'M': 1, '': 1, 'g': 1 << 10, 'G': 1 << 10}


def extract_runtime_limit(value):
    try:
        result = float(value)
    except:
        result = cre_runtime_limit.search(value).groupdict()
        result = runtime_scales[result['scale']] * float(result['value'])
    return timedelta(minutes=result)


def extract_memory_limit(value):
    try:
        return float(value)
    except:
        result = cre_memory_limit.search(value).groupdict()
        return memory_scales[result['scale']] * float(result['value'])

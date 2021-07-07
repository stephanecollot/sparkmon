"""Utilities functions."""
import collections
import math
import os
from typing import Any

import psutil


def convert_size(size_bytes) -> str:
    """Convert bytes in human readble string."""
    if size_bytes <= 0:
        return f"{size_bytes:.2f}B"
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return "%s %s" % (s, size_name[i])


def get_memory() -> Any:
    """Get current memory usage in bytes."""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss  # in bytes


def flatten_dict(d, parent_key="", sep="_"):
    """Flatten dict recursively."""
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

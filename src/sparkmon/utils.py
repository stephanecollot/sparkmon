"""Utilities functions."""
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

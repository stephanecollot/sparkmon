# Copyright (c) 2021 ING Wholesale Banking Advanced Analytics
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
# the Software, and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
# FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
# COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
# IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""Utilities functions."""
import collections
import getpass
import math
import os
from typing import Any

import psutil


def convert_size(size_bytes) -> str:
    """Convert bytes in human readble string.

    Credit: https://stackoverflow.com/a/14822210/1876485
    """
    if size_bytes == 0:
        return "0"
    elif size_bytes < 0:
        sign = -1
        size_bytes = size_bytes * -1
    else:
        sign = 1
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2) * sign
    return "%s %s" % (s, size_name[i])


def get_memory_process() -> Any:
    """Get memory usage in bytes of this process running Python."""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss  # in bytes


def get_memory_user() -> Any:
    """Get memory usage in bytes of all process of the current user."""
    user = getpass.getuser()
    # print(user)

    total = 0
    for p in psutil.process_iter():
        try:
            if p.username() == user:
                total += p.memory_info().rss
        except psutil.AccessDenied:
            # For some process we might permission issue, with the following error
            # psutil AccessDenied: psutil.AccessDenied (pid=599, name='PanGPS')
            pass

    return total


def flatten_dict(d, parent_key="", sep="_"):
    """Flatten dict recursively.

    Credit: https://stackoverflow.com/a/6027615/1876485
    """
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

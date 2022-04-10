"""We define the logger that is going to be used in the entire package.

We should not configure the logger, that is the responsability of the user.
By default in Python the log level is set to WARNING.
"""
import logging

log = logging.getLogger("sparkmon")

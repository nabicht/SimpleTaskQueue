"""
Copyright 2019 Peter F Nabicht, Big Shoulders Software
Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 documentation files (the "Software"), to deal in the Software without restriction, including without
 limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 the Software, and to permit persons to whom the Software is furnished to do so, subject to the following
 conditions:
The above copyright notice and this permission notice shall be included in all copies or substantial portions
 of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED
 TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF
 CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 DEALINGS IN THE SOFTWARE.
"""

import datetime
import logging


def basic_logger(log_file_name=None, file_level=None, console_level=None):
    if file_level is None and console_level is None:  # no log to configure so return None
        return None
    formatter = logging.Formatter('%(asctime)s: %(levelname)s: %(message)s')
    logger = logging.getLogger(__name__)
    if file_level is not None and console_level is not None:
        logger.setLevel(min(file_level, console_level))
    else:
        # both being None is handled above, so one of them needs to be not None here
        if file_level is not None:
            logger.setLevel(file_level)
        else:
            logger.setLevel(console_level)
    if console_level is not None:
        ch = logging.StreamHandler()
        ch.setLevel(console_level)
        ch.setFormatter(formatter)
        logger.addHandler(ch)
    if file_level is not None:
        if log_file_name is None:
            log_file_name = time_stamped_file_name("STQ")
        fh = logging.FileHandler(log_file_name, mode='a')
        fh.setLevel(file_level)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
    return logger


def time_stamped_file_name(log_file_prefix):
    return '%s_%s.log' % (log_file_prefix, datetime.datetime.now().strftime("%Y%m%d-%H%M%S"))

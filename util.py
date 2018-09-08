import datetime
import logging


def basic_logger(log_file_name, file_level=logging.INFO, console_level=logging.INFO):
    formatter = logging.Formatter('%(asctime)s: %(levelname)s: %(message)s')
    logger = logging.getLogger(__name__)
    logger.setLevel(min(file_level, console_level))
    # console handler for warning and worse
    ch = logging.StreamHandler()
    ch.setLevel(console_level)
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    # file handler for everything
    fh = logging.FileHandler(log_file_name, mode='a')
    fh.setLevel(file_level)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    return logger


def time_stamped_file_name(log_file_prefix):
    return '%s_%s.log' % (log_file_prefix, datetime.datetime.now().strftime("%Y%m%d-%H%M%S%f"))
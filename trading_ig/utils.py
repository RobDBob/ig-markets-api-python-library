#!/usr/bin/env python
# -*- coding:utf-8 -*-
import logging
import os
import logging
import traceback
import six

logger = logging.getLogger(__name__)


DATE_FORMATS = {1: "%Y:%m:%d-%H:%M:%S", 2: "%Y/%m/%d %H:%M:%S", 3: "%Y/%m/%d %H:%M:%S"}


def conv_resol(resolution):
    return resolution


def conv_datetime(dt, version=2):
    """Converts dt to string like
    version 1 = 2014:12:15-00:00:00
    version 2 = 2014/12/15 00:00:00
    version 3 = 2014/12/15 00:00:00
    """
    try:
        fmt = DATE_FORMATS[int(version)]
        return dt.strftime(fmt)
    except (ValueError, TypeError):
        logger.warning("conv_datetime returns %s" % dt)
        return dt


def conv_to_ms(td):
    """Converts td to integer number of milliseconds"""
    try:
        if isinstance(td, six.integer_types):
            return td
        else:
            return int(td.total_seconds() * 1000.0)
    except ValueError:
        logger.error(traceback.format_exc())
        logger.warning("conv_to_ms returns '%s'" % td)
        return td


def remove(cache):
    """Remove cache"""
    try:
        filename = "%s.sqlite" % cache
        print("remove %s" % filename)
        os.remove(filename)
    except Exception:
        pass



def create_logger(logger_name, file_name=None):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    file_handler = logging.FileHandler(file_name)
    formatter    = logging.Formatter('%(asctime)s(%(levelname)s): %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logging.getLogger(logger_name)
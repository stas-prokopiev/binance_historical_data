"""This is a one file library with main function initialize_project_logger
which can be used as first logger set up for new py packages
Manual can be found here: https://github.com/stas-prokopiev/basic_package_logger
"""
import sys
import os
import io
import logging

# Define constants with msg formats
# File msg formats
STR_DEBUG_FILE_FORMAT = (
    "{"
    "'levelname':'%(levelname)s', "
    "'asctime':'%(asctime)s', "
    "'filename':'%(filename)s', "
    "'name':'%(name)s', "
    "'funcName':'%(funcName)s', "
    "'lineno':'%(lineno)d', "
    "'message':'%(message)s'"
    "}"
)

STR_ERROR_FILE_FORMAT = STR_DEBUG_FILE_FORMAT
# stdout msg formats
STR_DEBUG_FORMAT = '[%(levelname)s] %(message)s'
STR_INFO_FORMAT = '%(message)s'
STR_WARNING_FORMAT = '[%(levelname)s] %(message)s'
STR_ERROR_FORMAT = (
    '[%(levelname)s: %(asctime)s:%(filename)s:'
    '%(name)s:%(funcName)s:%(lineno)d] %(message)s')


class OnlyLowerLevelFilter():
    """Define filter to show only logs with level lower than given level
    """
    def __init__(self, level):
        self.level = level

    def filter(self, record):
        """Filter messages with level lower than given"""
        return record.levelno < self.level


def initialize_project_logger(
        name,
        path_dir_where_to_store_logs="",
        is_stdout_debug=False,
        is_to_propagate_to_root_logger=False,
):
    """function returns a perfectly set up logger for the new package"""
    logger_obj = logging.getLogger(name)
    # If logger already exists then just return it
    if logger_obj.handlers:
        return logger_obj
    # Create and set basic settings for logger
    logger_obj.setLevel(20-int(is_stdout_debug)*10)
    logger_obj.propagate = is_to_propagate_to_root_logger
    # If root logger has not been set
    # Or user don't know what is logging at all
    # logging.basicConfig(file="/dev/null", level=0)  # Linux
    # logging.basicConfig(file="NUL", level=0)  # Win
    logging.basicConfig(stream=io.StringIO(), level=0)  # Any but eats memory
    #####
    # 1) Set up stdout logs
    # 1.0) Add debug handler if necessary
    if is_stdout_debug:
        debug_handler = logging.StreamHandler(sys.stdout)
        debug_handler.setLevel(level=0)
        debug_handler.setFormatter(logging.Formatter(STR_DEBUG_FORMAT))
        debug_handler.addFilter(OnlyLowerLevelFilter(20))
        logger_obj.addHandler(debug_handler)
    # 1.1) Add info handler
    info_handler = logging.StreamHandler(sys.stdout)
    info_handler.setLevel(level=20)
    info_handler.setFormatter(logging.Formatter(STR_INFO_FORMAT))
    info_handler.addFilter(OnlyLowerLevelFilter(30))
    logger_obj.addHandler(info_handler)
    # 1.2) Add warning handler
    warning_handler = logging.StreamHandler(sys.stdout)
    warning_handler.setLevel(level=30)
    warning_handler.setFormatter(logging.Formatter(STR_WARNING_FORMAT))
    warning_handler.addFilter(OnlyLowerLevelFilter(40))
    logger_obj.addHandler(warning_handler)
    # 1.3) Add error handler
    error_handler = logging.StreamHandler(sys.stderr)
    error_handler.setLevel(level=40)
    error_handler.setFormatter(logging.Formatter(STR_ERROR_FORMAT))
    logger_obj.addHandler(error_handler)
    if not path_dir_where_to_store_logs:
        return None
    if not os.path.isdir(path_dir_where_to_store_logs):
        raise TypeError(
            "Wrong type of argument 'path_dir_where_to_store_logs'\n"
            "Expected to get string with path to some directory "
            "But got: " + str(path_dir_where_to_store_logs)
        )
    #####
    # 2) Set up file handlers for logger
    # Create folder for Logs
    str_path_to_logs_dir = os.path.join(path_dir_where_to_store_logs, "Logs")
    if not os.path.isdir(str_path_to_logs_dir):
        os.makedirs(str_path_to_logs_dir)
    # 2.1) Debug handler
    debug_file_handler = logging.handlers.RotatingFileHandler(
        os.path.join(str_path_to_logs_dir, "debug.log"),
        maxBytes=10000,
        backupCount=1
    )
    debug_file_handler.setLevel(level=0)
    debug_file_handler.setFormatter(logging.Formatter(STR_DEBUG_FILE_FORMAT))
    logger_obj.addHandler(debug_file_handler)
    # 2.1) Warnings and above handler
    warnings_file_handler = logging.handlers.RotatingFileHandler(
        os.path.join(str_path_to_logs_dir, "errors.log"),
        maxBytes=10000,
        backupCount=1
    )
    warnings_file_handler.setLevel(level=30)
    warnings_file_handler.setFormatter(
        logging.Formatter(STR_ERROR_FILE_FORMAT))
    logger_obj.addHandler(warnings_file_handler)

__all__ = ["find_public_scripts", "configure_logging", "generate_logfile"]

import os
import logging
import time


def find_public_scripts(root):
    """Find all public scripts in the specified root path.

    Public scripts are executable files whose names do not start
    with "." or "_".

    Parameters
    ----------
    root : `str`, `bytes` or `os.PathLike`
        Path to root directory.

    Returns
    -------
    scripts : `list` of `str`
        Relative path of each public script found in ``root``.
    """
    paths = []
    for dirpath, dirnames, filenames in os.walk(root, topdown=True, followlinks=False):
        dirnames[:] = [name for name in dirnames if not name.startswith(".")]
        paths += [os.path.join(dirpath, filename) for filename in filenames if filename[0] not in (".", "_")]
    executables = [path for path in paths if os.access(path, os.X_OK)]
    return [os.path.relpath(exe, root) for exe in executables]


def configure_logging(verbose=0, console_format=None, filename=None):
    """Configure the logging for the system.

    Parameters
    ----------
    verbose : int
        Log level.
    console_format : str
        Format string for the console.
    filename : str
        A name, including path, for a log file. If None, will create a file.
    """
    console_detail = verbose
    file_detail = logging.DEBUG

    main_level = max(console_detail, file_detail)

    log_format = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    if console_format is None:
        console_format = log_format
    else:
        console_format = console_format

    logging.basicConfig(level=main_level, format=console_format)
    logging.captureWarnings(True)
    # Remove old console logger, as it will double up messages
    # when levels match.
    logging.getLogger().removeHandler(logging.getLogger().handlers[0])

    ch = logging.StreamHandler()
    ch.setLevel(console_detail)
    ch.setFormatter(logging.Formatter(console_format))
    logging.getLogger().addHandler(ch)

    log_file = logging.FileHandler(filename)
    log_file.setFormatter(logging.Formatter(log_format))
    log_file.setLevel(file_detail)
    logging.getLogger().addHandler(log_file)


def generate_logfile(basename="scriptqueue"):
    """Generate a log file name based on current time.
    """
    timestr = time.strftime("%Y-%m-%d_%H:%M:%S")
    log_path = os.path.expanduser('~/.{}/log'.format(basename))
    if not os.path.exists(log_path):
        os.makedirs(log_path)
    logfilename = os.path.join(log_path, "%s.%s.log" % (basename, timestr))
    return logfilename

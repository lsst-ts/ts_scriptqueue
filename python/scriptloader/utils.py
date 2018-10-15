__all__ = ["findscripts"]

import os


def findscripts(root):
    """Find all public scripts in the specified root path.

    Parameters
    ----------
    root : `str`, `bytes` or `os.PathLike`
        Path to root directory.

    Returns
    -------
    scripts : `iterable` of `str`
        Relative path of each public script found in ``root``.
        Public scripts are executable files whose names do not start
        with "." or "_".
    """
    paths = []
    for dirpath, dirnames, filenames in os.walk(root, topdown=True, followlinks=False):
        dirnames[:] = [name for name in dirnames if not name.startswith(".")]
        paths += [os.path.join(dirpath, filename) for filename in filenames if filename[0] not in (".", "_")]
    executables = [path for path in paths if os.access(path, os.X_OK)]
    return [os.path.relpath(exe, root) for exe in executables]

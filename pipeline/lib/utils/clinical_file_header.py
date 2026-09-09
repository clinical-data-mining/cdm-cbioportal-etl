"""
clinical_file_header.py

Helper for reading cBioPortal-format clinical files, which may or may not
carry the standard 4-row '#'-prefixed metadata block (display label,
description, datatype, priority) before the real column-name header row.
Not every source file has this block (e.g. some cohorts' clinical files
only gain it after a separate metadata-header-adding step), so the row
count can't be assumed to be a fixed number.
"""


def detect_header_row_count(fname: str) -> int:
    """
    Count leading '#'-prefixed metadata rows before the real header row.

    Parameters
    ----------
    fname : str
        Path to a tab-separated clinical file.

    Returns
    -------
    int
        Number of leading lines starting with '#'. 0 for a file with no
        metadata block (real header is line 1); pass this directly as
        pandas' `header=` argument to land on the real header row
        regardless of how many metadata lines precede it.
    """
    n = 0
    with open(fname) as f:
        for line in f:
            if line.startswith('#'):
                n += 1
            else:
                break
    return n

# Assign package metadata from __version__.py-file if it exists.
#
#  - This file is only created by build/release process.
#  - for local dev this files does not exist, and we use default values.
#
# For details about Python version formats, see:
#   https://peps.python.org/pep-0440/
try:
    from .__version__ import __version__
except:
    __version__ = "0.0.0+localdev"

try:
    from .__version__ import __git_sha__
except:
    __git_sha__ = 40 * "0"


def version_string():
    return f"v{__version__} ({__git_sha__[:6]})"

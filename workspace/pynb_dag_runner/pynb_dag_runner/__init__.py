# For details about Python version, see:
#   https://peps.python.org/pep-0440/
#
try:
    from .__version__ import __version__
except:
    __version__ = "0.0.0+local"

# --

# For dev snapshot releases define also __git_sha__.
# Otherwise set it to None
try:
    from .__version__ import __git_sha__
except:
    __git_sha__ = None

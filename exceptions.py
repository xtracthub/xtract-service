
import dill

from tblib import Traceback
from functools import wraps
from six import reraise

# Credit: Schema for errors borrowed from the Parsl Project.
# Link: https://github.com/Parsl/parsl/blob/bd5354d0381d45f390bd11cf16c150dee612c325/parsl/app/errors.py#L109


class XtractError(Exception):
    """ Base class for all exceptions.

    Only to be invoked when a more specific error is not available.
    """


class HttpsDownloadTimeout(Exception):
    """ A file did not reach the compute resource within n seconds, if
    user set timeout=n
    """


class ExtractorError(XtractError):
    """ The extractor did not successfully complete due to extractor file """


class RemoteExceptionWrapper:
    def __init__(self, e_type, e_value, traceback):
        self.e_type = dill.dumps
        self.e_value = dill.dumps(e_value)
        self.e_traceback = Traceback(traceback)

    def reraise(self):
        t = dill.loads(self.e_type)
        v = dill.loads(self.e_value)
        tb = self.e_traceback.as_traceback()

        reraise(t, v, tb)


def wrap_error(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        import sys
        from parsl.app.errors import RemoteExceptionWrapper
        try:
            return func(*args, **kwargs)
        except Exception:
            return RemoteExceptionWrapper(*sys.exc_info())
    return wrapper
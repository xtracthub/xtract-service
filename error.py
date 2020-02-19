# http://flask.pocoo.org/docs/1.0/patterns/apierrors/
class ApiError(Exception):
    status = 500

    def __init__(self, *errors, status=None):
        self.status = status if status is not None else self.status
        self.errors = errors

    def to_dict(self):
        errs = [{"detail": str(err)} for err in self.errors]
        return {"errors": errs}


class DeveloperError(ApiError):
    """
    Your action provider implementation is trying
    to return a response that conflicts with the API
    specification.
    """


class InvalidRequest(ApiError):
    status = 400


class NoAuthentication(ApiError):
    status = 401


class NotAuthorized(ApiError):
    status = 403


class NotFound(ApiError):
    status = 404


class InvalidState(ApiError):
    status = 409
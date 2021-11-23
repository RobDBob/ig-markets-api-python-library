class ApiExceededException(Exception):
    """Raised when our code hits the IG endpoint too often"""
    pass


class IGException(Exception):
    pass

class IGExceptionSessionReset(Exception):
    pass
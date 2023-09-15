class ScyllaPyBaseError(Exception):
    """Base scyllapy exception."""

class ScyllaPyDBError(ScyllaPyBaseError):
    """
    Database related exception.

    This exception can be thrown when
    the database returns an error.
    """

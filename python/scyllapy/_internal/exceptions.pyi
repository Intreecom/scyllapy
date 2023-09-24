class ScyllaPyBaseError(Exception):
    """Base scyllapy exception."""

class ScyllaPyBindingError(ScyllaPyBaseError):
    """
    Error that occurs during parameter binding.

    This error can be thrown if a parameter
    is not of the correct type and therefore cannot
    be bound.
    """

class ScyllaPyDBError(ScyllaPyBaseError):
    """
    Database related exception.

    This exception can be thrown when
    the database returns an error.
    """

class ScyllaPySessionError(ScyllaPyDBError):
    """
    Error related to database session.

    This exception can be thrown when
    session was not properly initialized,
    or if it was closed by some reason.
    """

class ScyllaPyMappingError(ScyllaPyBaseError):
    """
    Exception that occurs during mapping results back to python.

    It can be thrown if you request row fetching,
    but query didn't return any rows.

    Also it occurs if rows cannot be mapped to python types.
    """

class ScyllaPyQueryBuiderError(ScyllaPyBaseError):
    """
    Error that is thrown if Query cannot be built.

    When using query builder you can try to execute
    partialy built query that is guaranteed to
    have syntax errors. In order to avoid
    such situations we introduced another type of error,
    that is thrown before query is executed.
    """

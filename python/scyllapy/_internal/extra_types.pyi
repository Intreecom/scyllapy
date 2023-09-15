class BigInt:
    def __init__(self, val: int) -> None: ...

class SmallInt:
    def __init__(self, val: int) -> None: ...

class TinyInt:
    def __init__(self, val: int) -> None: ...

class Double:
    def __init__(self, val: float) -> None: ...

class Counter:
    def __init__(self, val: int) -> None: ...

class Unset:
    """
    Class for unsetting the variable.

    If you want to set NULL to a column,
    when performing INSERT statements,
    it's better to use Unset instead of setting
    NULL, because it may result in better performance.

    https://rust-driver.docs.scylladb.com/stable/queries/values.html#unset-values
    """

    def __init__(self) -> None: ...

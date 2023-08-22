import secrets
import string


def random_string(length: int) -> str:
    return "".join([secrets.choice(string.ascii_lowercase) for _ in range(length)])

"""Forward-only query credential redaction for application log output."""
import logging
import re
from urllib.parse import unquote_plus


_QUERY_FIELD = re.compile(r"([?&](?:amp;)?)([^\s?&;=<>\"']+)=([^\s&#<>\"']*)")
_CREDENTIAL_KEYS = {
    "token", "key", "apikey", "accesstoken", "authtoken", "authorization",
    "secret", "password", "signature", "sig", "clientsecret",
}


def redact_query_credentials(text: str) -> str:
    def redact(match):
        key = match[2]
        # Decode names, never values: encoded delimiters inside a secret must
        # remain part of the value being removed. Handle repeated parameters.
        for _ in range(len(key)):
            decoded = unquote_plus(key)
            if decoded == key:
                break
            key = decoded
        normalized = re.sub(r"[-_\s]", "", key).casefold()
        if normalized in _CREDENTIAL_KEYS:
            return f"{match[1]}{match[2]}=[REDACTED]"
        return match[0]

    return _QUERY_FIELD.sub(redact, text)


class RedactingFormatter(logging.Formatter):
    """Preserve Uvicorn's five access args and redact *after* traceback rendering."""
    def __init__(self, wrapped=None):
        super().__init__()
        self.wrapped = wrapped or logging.Formatter()

    def format(self, record):
        return redact_query_credentials(self.wrapped.format(record))


def install_log_redaction():
    # Uvicorn configures its handlers before importing app:app. Root handles
    # bridge and HTTPX logs; Uvicorn owns non-propagating access/error handlers.
    for name in (None, "uvicorn", "uvicorn.access", "uvicorn.error"):
        for handler in logging.getLogger(name).handlers:
            if not isinstance(handler.formatter, RedactingFormatter):
                handler.setFormatter(RedactingFormatter(handler.formatter))

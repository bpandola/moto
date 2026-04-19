"""timestreamquery base URL and path."""

from .responses import TimestreamQueryResponse

response = TimestreamQueryResponse()
url_bases = [
    r"https?://query\.timestream\.(.+)\.amazonaws\.com",
]
url_paths = {"{0}$": response.dispatch, "{0}/$": response.dispatch}

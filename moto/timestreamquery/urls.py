"""timestreamquery base URL and path."""

from .responses import TimestreamQueryResponse

url_bases = [
    r"https?://query\.timestream\.(.+)\.amazonaws\.com",
]

url_paths = {
    "{0}/?$": TimestreamQueryResponse.dispatch,
}

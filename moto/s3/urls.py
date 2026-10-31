from moto import settings

from .responses import S3Response

# Catch s3.amazonaws.com, but not s3-control.amazonaws.com
url_bases = [
    r"https?://s3(?!(-control|tables|vectors))(.*)\.amazonaws.com",
    r"https?://(?P<bucket_name>[a-zA-Z0-9\-_.]*)\.?s3(?!(-control|tables|vectors))(.*)\.amazonaws.com",
]

url_bases.extend(settings.get_s3_custom_endpoints())

url_paths = {
    "{0}/$": S3Response.dispatch,
    "{0}/.*$": S3Response.dispatch,
}

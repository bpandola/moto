from .responses import CloudWatchResponse

url_bases = [r"https?://monitoring\.(.+)\.amazonaws.com"]

url_paths = {
    "{0}/$": CloudWatchResponse.dispatch,
    # Smithy RPC v2 CBOR protocol: the service name is the targetPrefix in the botocore model
    "{0}/service/GraniteServiceVersion20100801/operation/(?P<operation>[^/]+)$": CloudWatchResponse.dispatch,
}

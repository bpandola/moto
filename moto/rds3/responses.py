import boto3

from moto.core.responses import BaseResponse
from . import utils
from .exceptions import InvalidParameterValue, RDSError
from .serialize import create_serializer


MAX_RECORDS = 100

# Do this once...
# Need to find a better place to put this, or cache it somehow.
client = boto3.client("rds", region_name="us-east-1")
api_to_method_mapping = {v: k for k, v in client.meta.method_to_api_mapping.items()}


class RDSResponse(BaseResponse):
    @property
    def backend(self):
        from .models import rds3_backends

        return rds3_backends[self.current_account][self.region]

    def __init__(self, *args):
        super().__init__()
        self.setup_class(*args)

    @classmethod
    def dispatch(cls, *args, **kwargs):
        instance = cls(*args)
        return instance.call_action()

    @property
    def parameters(self):
        return utils.parse_query_parameters(self._get_action(), self.querystring)

    def call_action(self):
        # Moved this section up to globals just for a bit of speed boost...
        # client = boto3.client("rds", region_name="us-east-1")
        # Use the boto ability to add methods/attrs to the client class to put this on there
        # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/events.html
        # Eh... will we have access to the meta attribute at that point?
        # api_to_method_mapping = {
        #     v: k for k, v in client.meta.method_to_api_mapping.items()
        # }
        action = self._get_action()
        operation_model = client.meta.service_model.operation_model(action)
        action_method = api_to_method_mapping[action]
        http_status_code = 200
        # backend = self.backend.backend  # Stupid new moto changes...
        if not hasattr(self.backend, action_method):
            raise NotImplementedError(f"The {action} action has not been implemented")
        try:
            result = getattr(self.backend, action_method)(**self.parameters)
            if client.can_paginate(action_method):  # or Marker or MaxRecords in params?
                result, marker = self._paginate_response(result)
            else:
                marker = None
            # This needs to be put under a more specific key (currently done in serializer)
            result_dict = {"result": result}
            if marker:
                result_dict["marker"] = marker
        except RDSError as e:
            result_dict = {"error": e}
            http_status_code = getattr(e, "http_status_code", 400)
        serializer = create_serializer("xml")
        serialized_xml = serializer.serialize_to_response(result_dict, operation_model)
        resp_headers = {"status": http_status_code}
        return http_status_code, resp_headers, serialized_xml

    def _paginate_response(self, resources):
        marker = self.parameters.get("marker")
        page_size = self.parameters.get("max_records", MAX_RECORDS)
        if page_size < 20 or page_size > 100:
            msg = (
                f"Invalid value {page_size} for MaxRecords. Must be between 20 and 100"
            )
            raise InvalidParameterValue(msg)
        all_resources = list(resources)
        all_ids = [resource.resource_id for resource in all_resources]
        if marker:
            start = all_ids.index(marker) + 1
        else:
            start = 0
        paginated_resources = all_resources[start : start + page_size]
        next_marker = None
        if len(all_resources) > start + page_size:
            next_marker = paginated_resources[-1].resource_id
        return paginated_resources, next_marker

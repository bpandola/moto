import json
from typing import Any
from urllib.parse import unquote

from moto.core.responses import BaseResponse

from .models import IoTDataPlaneBackend, iotdata_backends


class IoTDataPlaneResponse(BaseResponse):
    def __init__(self) -> None:
        super().__init__(service_name="iot-data")
        self.automated_parameter_parsing = True

    def setup_class(
        self, request: Any, full_url: str, headers: Any, use_raw_body: bool = False
    ) -> None:
        super().setup_class(request, full_url, headers, use_raw_body=True)

    def parse_parameters(self, request: Any) -> None:
        try:
            super().parse_parameters(request)
        except UnicodeDecodeError:
            # Some operations (e.g. Publish) accept raw binary payloads that
            # cannot be decoded as UTF-8.  Fall back to extracting the
            # topic from the URL path so that _get_param still works.
            self.params = {}
            if "/topics/" in self.path:
                topic = self.path.split("/topics/")[-1]
                self.params["topic"] = unquote(topic) if "%" in topic else topic

    @property
    def iotdata_backend(self) -> IoTDataPlaneBackend:
        return iotdata_backends[self.current_account][self.region]

    def update_thing_shadow(self) -> str:
        thing_name = self._get_param("thingName")
        shadow_name = self._get_param("shadowName")
        payload = self.iotdata_backend.update_thing_shadow(
            thing_name=thing_name,
            payload=self.body,
            shadow_name=shadow_name,
        )
        return json.dumps(payload.to_response_dict())

    def get_thing_shadow(self) -> str:
        thing_name = self._get_param("thingName")
        shadow_name = self._get_param("shadowName")
        payload = self.iotdata_backend.get_thing_shadow(
            thing_name=thing_name, shadow_name=shadow_name
        )
        return json.dumps(payload.to_dict())

    def delete_thing_shadow(self) -> str:
        thing_name = self._get_param("thingName")
        shadow_name = self._get_param("shadowName")
        payload = self.iotdata_backend.delete_thing_shadow(
            thing_name=thing_name, shadow_name=shadow_name
        )
        return json.dumps(payload.to_dict())

    def publish(self) -> str:
        topic = self._get_param("topic")
        self.iotdata_backend.publish(topic=topic, payload=self.body)
        return json.dumps({})

    def list_named_shadows_for_thing(self) -> str:
        thing_name = self._get_param("thingName")
        shadows = self.iotdata_backend.list_named_shadows_for_thing(thing_name)
        return json.dumps({"results": shadows})

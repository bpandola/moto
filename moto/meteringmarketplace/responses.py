import json
from datetime import datetime

from moto.core.responses import BaseResponse

from .models import MeteringMarketplaceBackend, meteringmarketplace_backends


class MarketplaceMeteringResponse(BaseResponse):
    def __init__(self) -> None:
        super().__init__(service_name="meteringmarketplace")
        self.automated_parameter_parsing = True

    @property
    def backend(self) -> MeteringMarketplaceBackend:
        return meteringmarketplace_backends[self.current_account][self.region]

    def batch_meter_usage(self) -> str:
        params = self._get_params()
        usage_records = params["UsageRecords"]
        for record in usage_records:
            if isinstance(record.get("Timestamp"), datetime):
                record["Timestamp"] = record["Timestamp"].isoformat()
        product_code = params["ProductCode"]
        results = self.backend.batch_meter_usage(product_code, usage_records)
        return json.dumps({"Results": results, "UnprocessedRecords": []})

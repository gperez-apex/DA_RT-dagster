from dagster import ConfigurableResource
import requests
from requests import Response

class ApiResource(ConfigurableResource):
    api: str

    def request(self, endpoint: str) -> Response:
        return requests.get(
            f"{self.api}/{endpoint}",
        )

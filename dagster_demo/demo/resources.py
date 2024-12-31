from dagster import ConfigurableResource, ExperimentalWarning
import requests
from requests import Response
import warnings

warnings.filterwarnings("ignore", category=ExperimentalWarning)


class ApiResource(ConfigurableResource):
    api: str

    def request(self, endpoint: str) -> Response:
        return requests.get(
            f"{self.api}/{endpoint}",
        )


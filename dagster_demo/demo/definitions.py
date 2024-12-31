from dagster import Definitions, load_assets_from_modules, EnvVar, ExperimentalWarning
from demo.resources import ApiResource
from demo import assets
import warnings

warnings.filterwarnings("ignore", category=ExperimentalWarning)

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources={
        "hackernews": ApiResource(api=EnvVar('SUPER_SECRET_API')),
    },
)


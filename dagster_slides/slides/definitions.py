from dagster import Definitions, load_assets_from_modules, EnvVar
from slides.resources import ApiResource
from slides import assets

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources={
        "hackernews": ApiResource(api=EnvVar('SUPER_SECRET_API')),
    }
)

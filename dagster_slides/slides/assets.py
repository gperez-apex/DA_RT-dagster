import json
import os
import pandas as pd
from slides.resources import ApiResource
from dagster import asset, AssetExecutionContext, MaterializeResult, MetadataValue

@asset
def topstory_ids(hackernews: ApiResource) -> None:
    top_new_story_ids = hackernews.request('topstories.json').json()[:20]
    os.makedirs("data", exist_ok=True)
    with open("data/topstory_ids.json", "w") as f:
        json.dump(top_new_story_ids, f)

@asset(deps=[topstory_ids])
def topstories(context: AssetExecutionContext, hackernews: ApiResource) -> None:
    '''Creates a dataframe with top 100 HackerNews stories.'''
    with open("data/topstory_ids.json", "r") as f:
        topstory_ids = json.load(f)

    results = []
    for item_id in topstory_ids:
        item = hackernews.request(f'item/{item_id}.json').json()
        results.append(item)

        if len(results) % 20 == 0:
            context.log.info(f"Got {len(results)} items so far.")

    df = pd.DataFrame(results)
    df.to_csv("data/topstories.csv")

    return MaterializeResult(
        metadata={
            "num_records": len(df),
            "preview": MetadataValue.md(df.head().to_markdown()),
        }
    )

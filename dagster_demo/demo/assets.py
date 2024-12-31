
import json

import requests
from dagster import (
    AssetCheckResult,
    AssetCheckSpec,
    AutoMaterializePolicy,
    AutoMaterializeRule,
    MaterializeResult,
    MetadataValue,
    asset,
    ExperimentalWarning
)

import warnings

warnings.filterwarnings("ignore", category=ExperimentalWarning)

TV_SHOW_FOR_ANALYSIS = "friends"

TV_SHOW_FILE = "show.json"
EPISODES_FILE = "episodes.json"
CHARACTERS_FILE = "characters.json"
CHARACTER_APPEARANCES_FILE = "character_appearances.json"
APPEARANCES_REPORT_FILE = "appearances.png"

@asset(
    auto_materialize_policy=AutoMaterializePolicy.eager().with_rules(
        AutoMaterializeRule.materialize_on_cron(cron_schedule="0 0 * * *")
    ),
    compute_kind="Python",
    group_name="ingestion",
)
def tv_show_info() -> MaterializeResult:
    """The top result from querying the TV Maze API for the TV show we want to analyze."""
    url_for_search = f"https://api.tvmaze.com/search/shows?q={TV_SHOW_FOR_ANALYSIS}"
    search_results = requests.get(url_for_search, verify=False).json()
    top_result = search_results[0]

    with open(TV_SHOW_FILE, "w") as f:
        f.write(json.dumps(top_result))

    return MaterializeResult(
        metadata={
            "request_url": MetadataValue.url(url_for_search),
            "top_result": MetadataValue.json(top_result),
            "top_result_search_score": MetadataValue.float(top_result["score"]),
            "total_results": MetadataValue.int(len(search_results)),
        }
    )

@asset(
    deps=[tv_show_info],
    compute_kind="Python",
    auto_materialize_policy=AutoMaterializePolicy.eager(),
    group_name="ingestion",
)
def episodes():
    """A JSON file containing all the episodes of the show. This is computed by querying the
    TV Maze API for the episodes of the show."""
    with open(TV_SHOW_FILE, "r") as f:
        show = json.loads(f.read())

    episodes_response = requests.get(
        f"https://api.tvmaze.com/shows/{show['show']['id']}/episodes",
        verify=False
    ).json()

    with open(EPISODES_FILE, "w") as f:
        f.write(json.dumps(episodes_response))

    # count the number of seasons
    seasons = set()
    for episode in episodes_response:
        seasons.add(episode["season"])

    return MaterializeResult(
        metadata={
            "request_url": MetadataValue.url(
                f"https://api.tvmaze.com/shows/{show['show']['id']}/episodes"
            ),
            "num_episodes": MetadataValue.int(len(episodes_response)),
            "num_seasons": MetadataValue.int(len(seasons)),
        }
    )

@asset(
    deps=[tv_show_info],
    compute_kind="Python",
    auto_materialize_policy=AutoMaterializePolicy.eager(),
    group_name="ingestion",
)
def characters() -> MaterializeResult:
    """A JSON file containing the names of all the characters in the show. This is computed by
    querying the TV Maze API for the cast of the show."""
    with open(TV_SHOW_FILE, "r") as f:
        show = json.loads(f.read())

    cast_response = requests.get(f"https://api.tvmaze.com/shows/{show['show']['id']}/cast", verify=False).json()

    characters = []
    for cast_member in cast_response:
        characters.append(cast_member["character"]["name"].split(" ")[0])

    with open(CHARACTERS_FILE, "w") as f:
        f.write(json.dumps(characters))

    return MaterializeResult(
        metadata={
            "request_url": MetadataValue.url(
                f"https://api.tvmaze.com/shows/{show['show']['id']}/cast"
            ),
            "response": MetadataValue.json(cast_response),
        }
    )

@asset(
    deps=[episodes, characters],
    check_specs=[AssetCheckSpec(name="at_least_one_character_mentioned", asset="character_appearances")],
    auto_materialize_policy=AutoMaterializePolicy.eager(),
    compute_kind="Python",
    group_name="analysis",
)
def character_appearances() -> MaterializeResult:
    """A JSON file containing the number of times each character is mentioned in the show.
    
    This is computed by counting the number of times each character's name appears in the summary of
    each episode."""
    with open(EPISODES_FILE, "r") as f:
        episodes_response = json.loads(f.read())

    with open(CHARACTERS_FILE, "r") as f:
        characters = json.loads(f.read())
        character_appearances = {character: 0 for character in characters}

    for episode in episodes_response:
        summary = episode["summary"]
        for character in character_appearances.keys():
            try:
                character_appearances[character] += summary.count(character)
            except:
                continue

    with open(CHARACTER_APPEARANCES_FILE, "w") as f:
        f.write(json.dumps(character_appearances))

    return MaterializeResult(
        metadata={"mentions": MetadataValue.json(character_appearances)},
        check_results=[
            AssetCheckResult(
                check_name="at_least_one_character_mentioned",
                passed=sum(character_appearances.values()) > 0,
            )
        ],
    )

@asset(
    deps=[character_appearances],
    compute_kind="Python",
    auto_materialize_policy=AutoMaterializePolicy.eager(),
    group_name="analysis",
)
def appearances_report() -> MaterializeResult:
    """A bar chart of the number of times each character is mentioned in the show. This bar chart
    is generated using [the QuickChart API](https://quickchart.io/)."""
    with open(CHARACTER_APPEARANCES_FILE, "r") as f:
        character_appearances = json.loads(f.read())
        character_appearances = {
            character: count for character, count in character_appearances.items() if count > 0
        }

    top_5_dict = get_top_5_keys_items(character_appearances)   

    report = requests.get(
        f"https://quickchart.io/chart?c={{type:'bar',data:{{labels:{list(top_5_dict.keys())},datasets:[{{label:'Appearances',data:{list(top_5_dict.values())}}}]}}}}",
        verify=False
    )

    with open("appearances.png", "wb") as f:
        f.write(report.content)

    return MaterializeResult(metadata={"chart_url": MetadataValue.url(report.url)})

def get_top_5_keys_items(data):
    top_5 = sorted(data.items(), key=lambda x: x[1], reverse=True)[:5]
    top_5_dict = dict(top_5)
    return top_5_dict
"""Pipeline to load slack into duckdb."""

from typing import List

import dlt
from pendulum import datetime, now
from slack import slack_source


def load_all_resources(start_date, end_date=None) -> None:
    """Load all resources from slack without any selection of channels."""

    pipeline = dlt.pipeline(
        pipeline_name="slack", destination='duckdb', dataset_name="akela_slack_backup_2"
    )

    source = slack_source(
        page_size=1000, start_date=start_date, end_date=end_date
    )
    source.root_key = True
    # print(source.resources["3-technical-help"])
    # asdf
    # source.resources["3-technical-help"].root_key = True
    source.resources["3-technical-help"].apply_hints(write_disposition="merge")

    # replies_tables = [resource for resource in source.resources.keys() if resource.endswith("_replies")]
    # pipeline.run(source.with_resources(replies_tables), write_disposition="merge")

    # asdf

    # Uncomment the following line to load only the access_logs resource. It is not selectes
    # by default because it is a resource just available on paid accounts.
    # source.access_logs.selected = True

    load_info = pipeline.run(
        source,
    )
    print(load_info)


def select_resource(selected_channels: List[str]) -> None:
    """Execute a pipeline that will load the given Slack list of channels with the selected
    channels incrementally beginning at the given start date."""

    pipeline = dlt.pipeline(
        pipeline_name="slack", destination='duckdb', dataset_name="slack_data"
    )

    source = slack_source(
        page_size=20,
        selected_channels=selected_channels,
        start_date=datetime(2023, 9, 1),
        end_date=datetime(2023, 9, 8),
    ).with_resources("channels", "1-announcements")

    load_info = pipeline.run(
        source,
    )
    print(load_info)


def get_users() -> None:
    """Execute a pipeline that will load Slack users list."""

    pipeline = dlt.pipeline(
        pipeline_name="slack", destination='duckdb', dataset_name="slack_data"
    )

    source = slack_source(
        page_size=20,
    ).with_resources("users")

    load_info = pipeline.run(
        source,
    )
    print(load_info)


if __name__ == "__main__":
    # Add your desired resources to the list...
    # resources = ["access_logs", "conversations", "conversations_history"]

    # load_all_resources(start_date=datetime(2000, 1, 1), end_date=datetime(2023, 12, 20))
    load_all_resources(start_date=now().subtract(days=10), end_date=now())
    # select_resource(selected_channels=["dlt-github-ci"])

    # select_resource(selected_channels=["1-announcements"])

    # get_users()

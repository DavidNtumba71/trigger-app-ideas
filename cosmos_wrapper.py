# pylint: disable=missing-module-docstring
# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring

import os
from dotenv import load_dotenv
from azure.cosmos import CosmosClient, PartitionKey, exceptions


def init_database():
    load_dotenv()
    url = os.getenv("COSMOS_URL")
    key = os.getenv("COSMOS_KEY")
    client = CosmosClient(url, credential=key)
    return client.get_database_client("demodb")


def init_container(container_name="movies", partition_key_name="primary_genre"):
    database = init_database()
    try:
        return database.create_container(
            container_name, PartitionKey(path=f"/{partition_key_name}")
        )
    except exceptions.CosmosResourceExistsError:
        return database.get_container_client(container_name)

# pylint: disable=missing-module-docstring
# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring

from custom_logging import log_info
from azure.cosmos import exceptions

class CosmosProcessor:
    def __init__(self, input_container, output_container):
        self.cosmos_input_container = input_container
        self.cosmos_output_container = output_container

    def get_documents_from(self, documents, of_type):
        return [doc for doc in documents if doc.get("type") == of_type]

    def execute_aggregation_idea(self, primary_genre, movies,  results_tracker):
        expected_view_id = f"__view__{primary_genre}".lstrip()
        view = None

        try:
            view = self.cosmos_input_container.read_item(
                item = expected_view_id,
                partition_key = primary_genre,
                response_hook = results_tracker.add_pointread_result
            )

        except exceptions.CosmosHttpResponseError:
            log_info("no view found. one will be created")
            view = self.generate_view(primary_genre, size=0)


        view['size'] = view['size'] + len(movies)
        output = self.cosmos_output_container.upsert_item(
            view, response_hook=results_tracker.add_upsert_result
        )

        return output

    def generate_view(self, primary_genre, size):
        return {
            "id": f"__view__{primary_genre}".lstrip(),
            "type": "VIEW",
            "primary_genre": primary_genre,
            "size": size,
        }

# pylint: disable=missing-module-docstring
# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring

from custom_logging import log_info

class CosmosProcessor:
    def __init__(self, input_container, output_container):
        self.cosmos_input_container = input_container
        self.cosmos_output_container = output_container

    def get_documents_from(self, documents, of_type):
        return [doc for doc in documents if doc.get("type") == of_type]

    def execute_aggregation_idea(self, primary_genre, results_tracker):
        documents = self.cosmos_input_container.query_items(
            query="SELECT * FROM movies",
            partition_key=primary_genre,
            populate_query_metrics=True,
            response_hook=results_tracker.add_query_result,
        )

        movies = self.get_documents_from(documents, of_type="MOVIE")
        log_info(f"genre '{primary_genre}' has {len(movies)} movies")

        try:
            # ! assuming that there is only one view
            view = self.get_documents_from(documents, of_type="VIEW")[0]
            view = self.update_view(view, size=len(movies))
        except IndexError:
            view = self.generate_view(primary_genre, size=len(movies))

        log_info(f"generated view for {primary_genre} as {view}")

        output = self.cosmos_output_container.upsert_item(
            view, response_hook=results_tracker.add_upsert_result
        )

        return output

    def update_view(self, view, size):
        # * prevents Cosmos Upsert error whereby document contains a space in id
        view["id"] = "__view__" if (view["id"] == "__view__ ") else view["id"]
        view["size"] = size
        return view

    def generate_view(self, primary_genre, size):
        return {
            "id": f"__view__{primary_genre}".lstrip(),
            "type": "VIEW",
            "primary_genre": primary_genre,
            "size": size,
        }

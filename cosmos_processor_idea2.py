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
        query_output = self.cosmos_input_container.query_items(
            query="SELECT COUNT(movies) FROM movies WHERE movies.Type LIKE 'MOVIE' ",
            partition_key=primary_genre,
            populate_query_metrics=True,
            response_hook=results_tracker.add_query_result,
        )
        document_count = query_output.next()
        movies = document_count["$1"] #? query result
        log_info(f"genre '{primary_genre}' has {movies} movies")
        view = self.generate_view(primary_genre, size=movies)
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

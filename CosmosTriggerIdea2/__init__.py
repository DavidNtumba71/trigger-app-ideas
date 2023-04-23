# pylint: disable=missing-module-docstring
# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring

from datetime import datetime
import azure.functions as func
from custom_logging import log_info
from cosmos_processor_idea2 import CosmosProcessor
from cosmos_wrapper import init_container
from results_tracker import ResultsTracker
from gspread_wrapper import GSpreadWrapper

APPLICATION = "AZURE_FUNCTION_IDEA2"


#! prevents recursive inserts
def passes_conditions(docuement, with_genre) -> bool:
    conditions = [
        lambda doc: doc.get("type") == "MOVIE",
        lambda doc: doc.get("primary_genre") == with_genre,
        lambda doc: doc.get("insert_type") == "COSMOS_BULK_INSERTS",
    ]
    return all([test(docuement) for test in conditions])


def main(documents: func.DocumentList) -> str:
    run_id = f'{datetime.utcnow().strftime("%Y%m%d%H:%M:%S")}_{APPLICATION}'
    distinct_genres = {movie["primary_genre"] for movie in documents}
    log_info(f"{APPLICATION}: found {len(distinct_genres)} genres to aggregate: {distinct_genres}")

    input_container = init_container()
    output_container = init_container("genre_summaries")

    processor = CosmosProcessor(input_container, output_container)
    results_tracker = ResultsTracker(APPLICATION, run_id)

    for genre in distinct_genres:
        movies = [doc for doc in documents if passes_conditions(doc, genre)]
        if movies:
            processor.execute_aggregation_idea(genre, results_tracker)

    results = results_tracker.results
    sheet = "Idea Results"
    if results:
        GSpreadWrapper(sheet=sheet).insert_rows(key_order = [],dictionaries=results)

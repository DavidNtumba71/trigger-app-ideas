# pylint: disable=missing-module-docstring
# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring
from datetime import datetime

class ResultsTracker:
    def __init__(self, app_name, run_id):
        self.app_name = app_name
        self.run_id = run_id
        self.results = []

    def add_pointread_result(self, headers, _):
        self.add_operation_result(headers, operation="POINTREAD")

    def add_query_result(self, headers, _):
       self.add_operation_result(headers, operation="QUERY")

    def add_upsert_result(self, headers, _):
        self.add_operation_result(headers, operation="UPSERT")

    def value_or_default(self, response_value, default = 0):
        if response_value is None:
            return default
        return float(response_value)

    def add_operation_result(self, headers, operation):
        ru_spent = self.value_or_default(headers.get("x-ms-request-charge"))
        ms_spent = self.value_or_default(headers.get("x-ms-request-duration-ms"))
        request_result =  {
            "Method": self.app_name,
            "RunId": self.run_id,
            "Request Type": operation,
            "Timestamp": datetime.utcnow().strftime("%m/%d/%Y, %H:%M:%S"),
            "RUs spent": ru_spent,
            "Milliseconds spent": ms_spent,
        }

        self.results.append(request_result)

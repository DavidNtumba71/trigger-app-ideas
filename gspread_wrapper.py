# pylint: disable=missing-module-docstring
# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring
import gspread


class GSpreadWrapper:
    def __init__(self, sheet):
        self.service_account = gspread.service_account()
        self.workbook = self.service_account.open("Cosmos AzureFunction Stats")
        self.worksheet_name = sheet
        self.worksheet = self.workbook.worksheet(sheet)

    def format_row_from(self, key_order, dictionary):
        if key_order:
            return [dictionary[key] for key in key_order]
        return list(dictionary.values())

    def insert_rows(self,key_order, dictionaries):
        print(f"now inserting values into {self.worksheet}")
        rows = []
        for dictionary in dictionaries:
            row = self.format_row_from(key_order, dictionary)
            rows.append(row)

        start_row = 2
        self.worksheet.insert_rows(
            values=rows, row=start_row, inherit_from_before=False)
        print(f"inserted {len(rows)} rows into {self.worksheet_name}")

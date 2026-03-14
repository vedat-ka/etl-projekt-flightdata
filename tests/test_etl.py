import logging
import unittest

from src.etl import FlightTransformer


class FlightTransformerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.logger = logging.getLogger("test_flight_transformer")
        self.transformer = FlightTransformer(self.logger)

    def test_transform_splits_arrival_timestamp_into_six_columns(self) -> None:
        records = [
            {
                "number": "BA 849",
                "status": "Expected",
                "airline": {"iata": "BA", "icao": "BAW", "name": "British Airways"},
                "departure": {
                    "airport": {"iata": "LHR", "countryCode": "gb"},
                },
                "arrival": {
                    "scheduledTime": {"local": "2026-03-11T11:15+01:00"},
                },
                "_query": {
                    "iata": "BER",
                    "airport_name": "Berlin Brandenburg",
                },
            }
        ]

        df = self.transformer.transform(records)

        self.assertEqual(len(df), 1)
        row = df.iloc[0]
        self.assertEqual(row["destination_airport_iata"], "BER")
        self.assertEqual(row["origin_country"], "gb")
        self.assertEqual(row["arrival_year"], 2026)
        self.assertEqual(row["arrival_month"], 3)
        self.assertEqual(row["arrival_day"], 11)
        self.assertEqual(row["arrival_hour"], 10)
        self.assertEqual(row["arrival_minute"], 15)
        self.assertEqual(row["arrival_second"], 0)

    def test_transform_removes_duplicate_source_keys(self) -> None:
        duplicate_record = {
            "number": "LH 133",
            "status": "Expected",
            "airline": {"iata": "LH", "icao": "DLH", "name": "Lufthansa"},
            "departure": {"airport": {"iata": "FRA", "countryCode": "de"}},
            "arrival": {"scheduledTime": {"local": "2026-03-11T12:40+01:00"}},
            "_query": {"iata": "CDG", "airport_name": "Paris Charles de Gaulle"},
        }

        df = self.transformer.transform([duplicate_record, duplicate_record.copy()])

        self.assertEqual(len(df), 1)


if __name__ == "__main__":
    unittest.main()
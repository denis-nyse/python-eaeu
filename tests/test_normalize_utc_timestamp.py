import unittest

from rest_api_eaeu.download_eaeu_odata_csv import build_odata_filter, normalize_utc_timestamp


class NormalizeUtcTimestampTests(unittest.TestCase):
    def test_accepts_year_only(self) -> None:
        self.assertEqual(normalize_utc_timestamp("2025"), "2025-01-01T00:00:00.00Z")

    def test_accepts_year_and_month(self) -> None:
        self.assertEqual(normalize_utc_timestamp("2025-06"), "2025-06-01T00:00:00.00Z")

    def test_accepts_iso_date_only(self) -> None:
        self.assertEqual(normalize_utc_timestamp("2025-06-24"), "2025-06-24T00:00:00.00Z")

    def test_accepts_local_date_format(self) -> None:
        self.assertEqual(normalize_utc_timestamp("24.06.2025"), "2025-06-24T00:00:00.00Z")

    def test_accepts_full_iso_with_z(self) -> None:
        self.assertEqual(normalize_utc_timestamp("2025-06-24T01:02:03Z"), "2025-06-24T01:02:03Z")

    def test_converts_plus_00_00_suffix_to_z(self) -> None:
        self.assertEqual(
            normalize_utc_timestamp("2025-06-24T01:02:03+00:00"),
            "2025-06-24T01:02:03Z",
        )

    def test_rejects_invalid_month_for_year_month_input(self) -> None:
        with self.assertRaisesRegex(ValueError, "Неверный месяц"):
            normalize_utc_timestamp("2025-13")

    def test_rejects_non_iso_garbage(self) -> None:
        with self.assertRaisesRegex(ValueError, "Неверный формат даты/времени"):
            normalize_utc_timestamp("hello")

    def test_build_filter_with_updated_from(self) -> None:
        updated_from = normalize_utc_timestamp("2025")
        self.assertEqual(
            build_odata_filter("RU", updated_from, True),
            "unifiedCountryCode/value eq 'RU' and "
            "resourceItemStatusDetails/updateDateTime ge 2025-01-01T00:00:00.00Z",
        )

    def test_build_filter_without_updated_from(self) -> None:
        self.assertEqual(
            build_odata_filter("BY", None, True),
            "unifiedCountryCode/value eq 'BY'",
        )

    def test_build_filter_without_server_updated_filter(self) -> None:
        self.assertEqual(
            build_odata_filter("KG", "2025-01-01T00:00:00.00Z", False),
            "unifiedCountryCode/value eq 'KG'",
        )


if __name__ == "__main__":
    unittest.main()

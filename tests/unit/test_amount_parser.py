"""
Unit tests cho AmountParser.
Chạy: pytest tests/unit/test_amount_parser.py -v
"""

import pytest
from transformation.staging.amount_parser import parse_amount, AmountFormat


class TestAmountParser:

    def test_us_format_with_dollar(self):
        r = parse_amount("$1,234.56")
        assert r.parsed_value == pytest.approx(1234.56)
        assert r.format == AmountFormat.US
        assert r.currency == "USD"

    def test_us_format_negative_dollar_dash(self):
        """$-77.00 — format dari data thực tế (transactions_data.csv)"""
        r = parse_amount("$-77.00")
        assert r.parsed_value == pytest.approx(-77.00)
        assert r.is_negative == True
        assert r.currency == "USD"

    def test_eu_format(self):
        r = parse_amount("1.234,56")
        assert r.parsed_value == pytest.approx(1234.56)
        assert r.format == AmountFormat.EU

    def test_clean_numeric(self):
        r = parse_amount("1234.56")
        assert r.parsed_value == pytest.approx(1234.56)
        assert r.format == AmountFormat.CLEAN

    def test_vnd_with_symbol(self):
        r = parse_amount("₫200,000")
        assert r.parsed_value == pytest.approx(200000.0)
        assert r.currency == "VND"
        assert r.format == AmountFormat.VN

    def test_accounting_negative(self):
        r = parse_amount("(500.00)")
        assert r.parsed_value == pytest.approx(-500.0)
        assert r.is_negative == True

    def test_ambiguous_single_comma_3digits(self):
        """1,234 — không biết 1234 hay 1.234"""
        r = parse_amount("1,234")
        assert r.parsed_value is None
        assert r.format == AmountFormat.AMBIGUOUS

    def test_ambiguous_single_dot_3digits(self):
        """1.234 — không biết 1234 hay 1.234"""
        r = parse_amount("1.234")
        assert r.parsed_value is None
        assert r.format == AmountFormat.AMBIGUOUS

    def test_vnd_context_resolves_ambiguous(self):
        """1,234 với VND → chắc chắn là 1234"""
        r = parse_amount("₫1,234")
        assert r.parsed_value == pytest.approx(1234.0)
        assert r.format == AmountFormat.VN

    def test_null_input(self):
        r = parse_amount(None)
        assert r.parsed_value is None
        assert r.format == AmountFormat.INVALID

    def test_placeholder_na(self):
        r = parse_amount("N/A")
        assert r.parsed_value is None
        assert r.format == AmountFormat.INVALID

    def test_multiple_dots_vn_style(self):
        """1.234.567 — VN style, multiple dots"""
        r = parse_amount("1.234.567")
        assert r.parsed_value == pytest.approx(1234567.0)
        assert r.format == AmountFormat.VN

    def test_multiple_commas_us_style(self):
        """1,234,567 — US style, multiple commas"""
        r = parse_amount("1,234,567")
        assert r.parsed_value == pytest.approx(1234567.0)
        assert r.format == AmountFormat.US

    def test_eu_with_decimal_comma(self):
        r = parse_amount("1234,56")
        assert r.parsed_value == pytest.approx(1234.56)
        assert r.format == AmountFormat.EU

    def test_positive_number(self):
        r = parse_amount("$14.57")
        assert r.parsed_value == pytest.approx(14.57)
        assert r.is_negative == False

"""
AmountParser — parse raw amount strings thành numeric values.

Logic: Detect → Classify → Parse → Flag nếu ambiguous.
Không bao giờ tự sửa khi không chắc chắn.
Không bao giờ drop record.
"""

import re
from enum import Enum
from dataclasses import dataclass
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


class AmountFormat(Enum):
    US        = "US"         # "1,234.56"  → phẩy=thousand, chấm=decimal
    EU        = "EU"         # "1.234,56"  → chấm=thousand, phẩy=decimal
    CLEAN     = "CLEAN"      # "1234.56"   → không separator
    VN        = "VN"         # "200,000"   với currency VND
    AMBIGUOUS = "AMBIGUOUS"  # "1,234" hoặc "1.234" → không biết
    INVALID   = "INVALID"    # null, "N/A", không parse được


@dataclass
class ParseResult:
    raw_value:    str
    parsed_value: Optional[float]
    format:       AmountFormat
    currency:     Optional[str]
    is_negative:  bool
    note:         str


def parse_amount(raw: Optional[str]) -> ParseResult:
    """
    Core parsing logic. Xử lý 1 raw amount string.
    Trả về ParseResult với đầy đủ metadata.
    """
    if raw is None:
        return ParseResult(raw, None, AmountFormat.INVALID, None, False, "null input")

    original = raw.strip()
    s = original

    if not s or s in ("-", "N/A", "n/a", "NULL", "null", "none", ""):
        return ParseResult(original, None, AmountFormat.INVALID, None, False, "empty or placeholder")

    # 1. Extract currency
    currency = None
    currency_map = [
        (r'^\$',  'USD'), (r'^€', 'EUR'), (r'^£', 'GBP'),
        (r'^₫',   'VND'), (r'^¥', 'JPY'),
        (r'\b(USD|EUR|GBP|VND|JPY|SGD|THB)\b', None),
    ]
    for pattern, code in currency_map:
        m = re.search(pattern, s, re.IGNORECASE)
        if m:
            currency = code if code else m.group(1).upper()
            s = re.sub(pattern, '', s, flags=re.IGNORECASE).strip()
            break

    # 2. Handle negative: "(500.00)" hoặc "-500" hoặc "$-77.00" (sau khi strip $)
    is_negative = False
    if s.startswith('(') and s.endswith(')'):
        is_negative = True
        s = s[1:-1].strip()
    elif s.startswith('-'):
        is_negative = True
        s = s[1:].strip()

    # 3. Remove spaces (French format: "1 234,56")
    s = s.replace(' ', '').replace('\xa0', '')

    # 4. Classify và parse
    has_dot   = '.' in s
    has_comma = ',' in s
    dot_cnt   = s.count('.')
    comma_cnt = s.count(',')

    sign = -1 if is_negative else 1

    # No separator → CLEAN
    if not has_dot and not has_comma:
        try:
            val = float(s)
            return ParseResult(original, sign * val, AmountFormat.CLEAN, currency, is_negative, "clean numeric")
        except ValueError:
            return ParseResult(original, None, AmountFormat.INVALID, currency, is_negative, f"cannot parse: {s}")

    # Both dot and comma → nhìn cái nào ở cuối = decimal separator
    if has_dot and has_comma:
        if s.rfind('.') > s.rfind(','):
            # "1,234.56" → US
            try:
                val = float(s.replace(',', ''))
                return ParseResult(original, sign * val, AmountFormat.US, currency, is_negative, "US format")
            except ValueError:
                return ParseResult(original, None, AmountFormat.INVALID, currency, is_negative, f"US parse failed: {s}")
        else:
            # "1.234,56" → EU
            try:
                val = float(s.replace('.', '').replace(',', '.'))
                return ParseResult(original, sign * val, AmountFormat.EU, currency, is_negative, "EU format")
            except ValueError:
                return ParseResult(original, None, AmountFormat.INVALID, currency, is_negative, f"EU parse failed: {s}")

    # Only dot
    if has_dot and not has_comma:
        if dot_cnt > 1:
            # "1.234.567" → multiple dots = thousand separator (VN style)
            try:
                val = float(s.replace('.', ''))
                return ParseResult(original, sign * val, AmountFormat.VN, currency, is_negative, "multiple dots = thousand sep")
            except ValueError:
                return ParseResult(original, None, AmountFormat.INVALID, currency, is_negative, f"multi-dot parse failed: {s}")
        # Single dot
        parts = s.split('.')
        decimal_len = len(parts[1]) if len(parts) > 1 else 0
        if decimal_len == 3:
            # "1.234" → ambiguous (EU thousand hoặc decimal?)
            if currency == 'VND':
                try:
                    val = float(s.replace('.', ''))
                    return ParseResult(original, sign * val, AmountFormat.VN, currency, is_negative, "VND → dot is thousand sep")
                except ValueError:
                    pass
            return ParseResult(original, None, AmountFormat.AMBIGUOUS, currency, is_negative,
                               f"ambiguous: '{s}' could be {s.replace('.', '')} or {s}")
        try:
            val = float(s)
            return ParseResult(original, sign * val, AmountFormat.CLEAN, currency, is_negative, "decimal number")
        except ValueError:
            return ParseResult(original, None, AmountFormat.INVALID, currency, is_negative, f"cannot parse: {s}")

    # Only comma
    if has_comma and not has_dot:
        if comma_cnt > 1:
            # "1,234,567" → US thousand separator
            try:
                val = float(s.replace(',', ''))
                return ParseResult(original, sign * val, AmountFormat.US, currency, is_negative, "multiple commas = thousand sep")
            except ValueError:
                return ParseResult(original, None, AmountFormat.INVALID, currency, is_negative, f"multi-comma parse failed: {s}")
        parts = s.split(',')
        decimal_len = len(parts[1]) if len(parts) > 1 else 0
        if decimal_len == 3:
            if currency == 'VND':
                try:
                    val = float(s.replace(',', ''))
                    return ParseResult(original, sign * val, AmountFormat.VN, currency, is_negative, "VND → comma is thousand sep")
                except ValueError:
                    pass
            return ParseResult(original, None, AmountFormat.AMBIGUOUS, currency, is_negative,
                               f"ambiguous: '{s}' could be {s.replace(',', '')} or {s.replace(',', '.')}")
        try:
            val = float(s.replace(',', '.'))
            return ParseResult(original, sign * val, AmountFormat.EU, currency, is_negative, "EU decimal comma")
        except ValueError:
            return ParseResult(original, None, AmountFormat.INVALID, currency, is_negative, f"cannot parse: {s}")

    return ParseResult(original, None, AmountFormat.INVALID, currency, is_negative, "unhandled format")


# ── Spark UDF wrapper ──────────────────────────────────────────────────────

def apply_amount_parser(df: DataFrame, raw_col: str = "amount") -> DataFrame:
    """
    Apply AmountParser lên toàn bộ DataFrame.
    Trả về DataFrame với 5 cột amount thay vì 1.

    Dùng Pandas UDF (vectorized) thay vì Python UDF thông thường:
    - Python UDF: serialize từng row qua Python-JVM boundary → chậm
    - Pandas UDF: serialize theo batch (arrow format) → nhanh hơn 10-100x
    """
    import pandas as pd
    from pyspark.sql.functions import pandas_udf
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType

    result_schema = StructType([
        StructField("amount",             DoubleType(),  True),
        StructField("amount_currency",    StringType(),  True),
        StructField("amount_format",      StringType(),  False),
        StructField("amount_parse_note",  StringType(),  True),
    ])

    @pandas_udf(result_schema)
    def _parse_udf(series: pd.Series) -> pd.DataFrame:
        results = []
        for raw in series:
            r = parse_amount(raw)
            results.append({
                "amount":            r.parsed_value,
                "amount_currency":   r.currency,
                "amount_format":     r.format.value,
                "amount_parse_note": r.note,
            })
        return pd.DataFrame(results)

    parsed = df.withColumn("_parsed", _parse_udf(F.col(raw_col)))

    return df \
        .withColumnRenamed(raw_col, "amount_raw") \
        .withColumn("amount",            F.col("_parsed.amount")) \
        .withColumn("amount_currency",   F.col("_parsed.amount_currency")) \
        .withColumn("amount_format",     F.col("_parsed.amount_format")) \
        .withColumn("amount_parse_note", F.col("_parsed.amount_parse_note")) \
        .drop("_parsed")

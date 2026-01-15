import re
from typing import List, Tuple

CardIssuer = str

# Card detection rules based on IIN/BIN ranges from the stripe documentation
# https://docs.stripe.com/testing#cards
CARD_DETECTION_RULES: List[Tuple[CardIssuer, re.Pattern]] = [
    ("Visa (debit)", re.compile(r"^400005")),
    ("Visa", re.compile(r"^4\d{12,18}$")),

    ("Mastercard (prepaid)", re.compile(r"^510510")),
    ("Mastercard (debit)", re.compile(r"^520082")),
    ("Mastercard (2-series)", re.compile(r"^2(?:2[2-9][0-9]|[3-6][0-9]{2}|7[01][0-9]|720)")),
    ("Mastercard", re.compile(r"^5[1-5]")),

    ("American Express", re.compile(r"^3[47]")),

    ("Discover (debit)", re.compile(r"^601198")),
    ("Discover", re.compile(r"^(?:6011|65|64[4-9])")),

    ("Diners Club (14-digit card)", re.compile(r"^36\d{12}$")),
    ("Diners Club", re.compile(r"^3(?:0[0-5]|[68]\d)")),

    ("BCcard and DinaCard", re.compile(r"^6555")),

    ("JCB", re.compile(r"^(?:2131|1800)\d{11}$|^35\d{14}$")),

    ("UnionPay (19-digit card)", re.compile(r"^62\d{17}$")),
    ("UnionPay (debit)", re.compile(r"^620000")),
    ("UnionPay", re.compile(r"^62")),
]


def detect_card_issuer(raw: str) -> CardIssuer:
    """Return the card issuer name based on prefix rules.

    Based on IIN/BIN ranges from the stripe documentation
    https://docs.stripe.com/testing#cards
    """
    num = re.sub(r"\D", "", raw)
    if not num:
        return "Unknown"

    for issuer, pattern in CARD_DETECTION_RULES:
        if pattern.match(num):
            return issuer

    return "Unknown"

from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, Literal

@dataclass
class Transaction:
    """Defines the transaction data structure."""
    transaction_id: str  # UUID format
    timestamp: datetime  # ISO format
    customer_id: str
    merchant_id: str
    merchant_category: Literal["retail", "food_service", "entertainment", "transportation", "government"]
    payment_method: Literal["online", "pos", "mobile", "nfc"]
    amount: float  # IRR currency
    location: Dict[str, float]  # {"lat": 35.7219, "lng": 51.3347}
    device_info: Optional[Dict[str, str]]  # Only for online/mobile payments
    status: Literal["approved", "declined", "pending"]
    commission_type: Literal["flat", "progressive", "tiered"]
    commission_amount: float
    vat_amount: float
    total_amount: float   # amount + vat_amount + commission_amount
    customer_type: Literal["individual", "CIP", "business"]
    risk_level: int  # 1-5
    failure_reason: Optional[Literal["cancelled", "insufficient_funds", "system_error", "fraud_prevented"]]
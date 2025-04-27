from datetime import datetime, timedelta
from typing import Optional, Dict
import json
from pydantic import BaseModel, ValidationError
from src.data_ingestion.schemas import Transaction


class TransactionModel(BaseModel):
    """Pydantic model for validation."""
    transaction_id: str
    timestamp: str  # Keep as string for initial validation
    customer_id: str
    merchant_id: str
    merchant_category: str
    payment_method: str
    amount: float
    location: Dict[str, float]
    device_info: Optional[Dict[str, str]]
    status: str
    commission_type: str
    commission_amount: float
    vat_amount: float
    total_amount: float
    customer_type: str
    risk_level: int
    failure_reason: Optional[str]

    class Config:
        extra = "forbid"  # Rejects unexpected fields

def parse_transaction(message_value: str) -> Optional[Transaction]:
    """Convert raw Kafka message to Transaction object with proper timestamp parsing + validation"""
    try:
        data = json.loads(message_value)
        validated = TransactionModel(**data).dict()
        
        # Convert timestamp string to datetime
        timestamp_str = validated["timestamp"].replace("Z", "")
        try:
            timestamp = datetime.fromisoformat(timestamp_str)
        except ValueError:
            # Handle different timestamp formats if needed
            timestamp = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S.%f")
        
        return Transaction(
            timestamp=timestamp,
            **{k: v for k, v in validated.items() if k != "timestamp"}
        )
    except (json.JSONDecodeError, ValidationError, ValueError) as e:
        print(f"Validation failed: {e}")
        return None
    

def validate_business_rules(transaction: Transaction) -> list[str]:
    """Check business rules from the PDF."""
    errors = []
    
    # Rule 1: Amount consistency
    calculated = transaction.amount + transaction.vat_amount + transaction.commission_amount
    if calculated != transaction.total_amount:
        errors.append("ERR_AMOUNT")
    
    # Rule 2: Time warping
    now = datetime.utcnow()
    if (transaction.timestamp > now) or ((now - transaction.timestamp) > timedelta(days=1)):
        errors.append("ERR_TIME")
    
    # Rule 3: Device mismatch
    if (transaction.payment_method == "mobile" and 
        transaction.device_info and 
        transaction.device_info.get("os") not in ["iOS", "Android"]):
        errors.append("ERR_DEVICE")

    
    return errors


def transaction_to_serializable(transaction: Transaction) -> dict:
    """Convert Transaction object to JSON-serializable dict"""
    data = transaction.__dict__.copy()
    data["timestamp"] = data["timestamp"].isoformat()
    return data
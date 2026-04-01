from fastapi import FastAPI, HTTPException
from sqlalchemy.orm import sessionmaker
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.db_setup import get_session, Transaction, FraudPrediction
from typing import List, Optional
from pydantic import BaseModel
from datetime import datetime

app = FastAPI(title="Fraud Detection API", version="1.0.0")

class TransactionResponse(BaseModel):
    transaction_id: str
    card_id: str
    amount: float
    merchant: str
    country: str
    timestamp: datetime
    transaction_type: str
    risk_score: Optional[float] = None
    fraud_label: Optional[bool] = None

@app.get("/")
def read_root():
    return {"message": "Fraud Detection API"}

@app.get("/transactions", response_model=List[TransactionResponse])
def get_transactions(limit: int = 100, offset: int = 0):
    """
    Get recent transactions with fraud predictions
    """
    try:
        session = get_session()

        # Join transactions with predictions
        results = session.query(
            Transaction.transaction_id,
            Transaction.card_id,
            Transaction.amount,
            Transaction.merchant,
            Transaction.country,
            Transaction.timestamp,
            Transaction.transaction_type,
            FraudPrediction.risk_score,
            FraudPrediction.fraud_label
        ).join(
            FraudPrediction,
            Transaction.transaction_id == FraudPrediction.transaction_id,
            isouter=True
        ).order_by(Transaction.timestamp.desc()) \
         .limit(limit).offset(offset).all()

        session.close()

        transactions = []
        for row in results:
            transactions.append(TransactionResponse(
                transaction_id=row.transaction_id,
                card_id=row.card_id,
                amount=row.amount,
                merchant=row.merchant,
                country=row.country,
                timestamp=row.timestamp,
                transaction_type=row.transaction_type,
                risk_score=row.risk_score,
                fraud_label=row.fraud_label
            ))

        return transactions

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/transactions/{transaction_id}", response_model=TransactionResponse)
def get_transaction(transaction_id: str):
    """
    Get specific transaction by ID
    """
    try:
        session = get_session()

        result = session.query(
            Transaction.transaction_id,
            Transaction.card_id,
            Transaction.amount,
            Transaction.merchant,
            Transaction.country,
            Transaction.timestamp,
            Transaction.transaction_type,
            FraudPrediction.risk_score,
            FraudPrediction.fraud_label
        ).join(
            FraudPrediction,
            Transaction.transaction_id == FraudPrediction.transaction_id,
            isouter=True
        ).filter(Transaction.transaction_id == transaction_id).first()

        session.close()

        if not result:
            raise HTTPException(status_code=404, detail="Transaction not found")

        return TransactionResponse(
            transaction_id=result.transaction_id,
            card_id=result.card_id,
            amount=result.amount,
            merchant=result.merchant,
            country=result.country,
            timestamp=result.timestamp,
            transaction_type=result.transaction_type,
            risk_score=result.risk_score,
            fraud_label=result.fraud_label
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stats")
def get_stats():
    """
    Get fraud detection statistics
    """
    try:
        session = get_session()

        total_transactions = session.query(Transaction).count()
        fraud_predictions = session.query(FraudPrediction).filter(FraudPrediction.fraud_label == True).count()
        avg_risk_score = session.query(FraudPrediction).filter(FraudPrediction.risk_score.isnot(None)).all()

        if avg_risk_score:
            avg_risk = sum([p.risk_score for p in avg_risk_score]) / len(avg_risk_score)
        else:
            avg_risk = 0

        session.close()

        return {
            "total_transactions": total_transactions,
            "fraud_transactions": fraud_predictions,
            "fraud_rate": fraud_predictions / total_transactions if total_transactions > 0 else 0,
            "average_risk_score": avg_risk
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
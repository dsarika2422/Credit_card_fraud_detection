import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, roc_auc_score
from sklearn.preprocessing import StandardScaler
import joblib
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.config import MODEL_PATH
from utils.feature_engineering import FeatureEngineer

def generate_sample_data(n_samples=10000):
    """
    Generate sample transaction data for training
    """
    np.random.seed(42)

    # Generate synthetic data
    data = {
        'transaction_amount': np.random.exponential(100, n_samples),
        'merchant_category': np.random.choice(['Retail', 'Online', 'Food', 'Travel', 'Electronics'], n_samples),
        'country': np.random.choice(['US', 'CA', 'UK', 'DE', 'FR', 'RU', 'CH'], n_samples),
        'transaction_frequency': np.random.poisson(2, n_samples),
        'amount_avg': np.random.exponential(80, n_samples),
        'amount_std': np.random.exponential(50, n_samples),
        'country_change': np.random.choice([0, 1], n_samples, p=[0.8, 0.2]),
        'time_diff': np.random.exponential(3600, n_samples),  # seconds
        'timestamp': pd.date_range('2023-01-01', periods=n_samples, freq='1H')[:n_samples],  # Add timestamp
        'card_id': np.random.choice(['1234567890123456', '9876543210987654'], n_samples)  # Add card_id
    }

    df = pd.DataFrame(data)

    # Create fraud labels (simplified logic)
    fraud_conditions = (
        (df['transaction_amount'] > 500) |
        (df['country_change'] == 1) |
        (df['transaction_frequency'] > 5) |
        ((df['country'] != 'US') & (df['transaction_amount'] > 200))
    )

    df['is_fraud'] = fraud_conditions.astype(int)

    # Add some noise to make it more realistic
    noise = np.random.choice([0, 1], n_samples, p=[0.95, 0.05])
    df['is_fraud'] = df['is_fraud'] | noise

    return df

def train_model():
    """
    Train fraud detection model
    """
    print("Generating sample data...")
    df = generate_sample_data()

    print(f"Dataset shape: {df.shape}")
    print(f"Fraud rate: {df['is_fraud'].mean():.3f}")

    # Feature engineering
    feature_engineer = FeatureEngineer()
    features = feature_engineer.engineer_features(df)

    # Prepare features and target
    X = features
    y = df['is_fraud']

    # Encode categorical features
    X = pd.get_dummies(X, columns=['merchant_category', 'country'], drop_first=True)

    # Split data
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    # Scale features
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    # Train model
    print("Training Random Forest model...")
    model = RandomForestClassifier(
        n_estimators=100,
        max_depth=10,
        random_state=42,
        class_weight='balanced'
    )

    model.fit(X_train_scaled, y_train)

    # Evaluate
    y_pred = model.predict(X_test_scaled)
    y_pred_proba = model.predict_proba(X_test_scaled)[:, 1]

    print("\nModel Performance:")
    print(classification_report(y_test, y_pred))
    print(f"AUC-ROC: {roc_auc_score(y_test, y_pred_proba):.3f}")

    # Save model and scaler
    os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)

    model_data = {
        'model': model,
        'scaler': scaler,
        'feature_columns': X.columns.tolist()
    }

    joblib.dump(model_data, MODEL_PATH)
    print(f"Model saved to {MODEL_PATH}")

    return model, scaler

if __name__ == "__main__":
    train_model()
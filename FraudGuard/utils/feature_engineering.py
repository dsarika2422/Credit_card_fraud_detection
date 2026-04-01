import pandas as pd
import numpy as np
from datetime import datetime
from utils.config import FEATURE_COLUMNS

class FeatureEngineer:
    def __init__(self):
        self.card_history = {}  # Store transaction history per card

    def engineer_features(self, transaction_df):
        """
        Engineer features for fraud detection from transaction data
        """
        df = transaction_df.copy()

        # Convert timestamp to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        # Sort by card_id and timestamp
        df = df.sort_values(['card_id', 'timestamp'])

        # Group by card_id for historical features
        grouped = df.groupby('card_id')

        # Transaction frequency (transactions per hour in last 24 hours)
        df['transaction_frequency'] = grouped['timestamp'].transform(
            lambda x: self._calculate_frequency(x)
        )

        # Amount statistics
        df['amount_avg'] = grouped['transaction_amount'].transform(
            lambda x: x.expanding().mean()
        )
        df['amount_std'] = grouped['transaction_amount'].transform(
            lambda x: x.expanding().std().fillna(0)
        )

        # Country change detection
        df['country_change'] = grouped['country'].transform(
            lambda x: (x != x.shift(1)).astype(int)
        )

        # Time difference between transactions
        df['time_diff'] = grouped['timestamp'].transform(
            lambda x: (x - x.shift(1)).dt.total_seconds().fillna(0)
        )

        # Normalize categorical features
        df['merchant_category_encoded'] = pd.Categorical(df['merchant_category']).codes
        df['country_encoded'] = pd.Categorical(df['country']).codes

        # Select features for model
        feature_df = df[FEATURE_COLUMNS].copy()

        return feature_df

    def _calculate_frequency(self, timestamps):
        """
        Calculate transaction frequency per hour in last 24 hours
        """
        frequencies = []
        for i, ts in enumerate(timestamps):
            # Count transactions in last 24 hours
            last_24h = timestamps[:i+1][timestamps[:i+1] >= ts - pd.Timedelta(hours=24)]
            freq = len(last_24h) / 24.0  # per hour
            frequencies.append(freq)
        return pd.Series(frequencies, index=timestamps.index)

    def update_history(self, card_id, transaction):
        """
        Update transaction history for a card
        """
        if card_id not in self.card_history:
            self.card_history[card_id] = []

        self.card_history[card_id].append(transaction)

        # Keep only last 100 transactions per card
        if len(self.card_history[card_id]) > 100:
            self.card_history[card_id] = self.card_history[card_id][-100:]

    def get_card_history(self, card_id):
        """
        Get transaction history for a card
        """
        return self.card_history.get(card_id, [])
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import time
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.db_setup import get_session, Transaction, FraudPrediction
from sqlalchemy import func

st.set_page_config(page_title="Fraud Detection Dashboard", page_icon="🔍", layout="wide")

def load_data():
    """
    Load transaction and fraud prediction data from database
    """
    session = get_session()

    # Get transactions with predictions
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
    ).order_by(Transaction.timestamp.desc()).limit(1000).all()

    session.close()

    # Convert to DataFrame
    data = []
    for row in results:
        data.append({
            'transaction_id': row.transaction_id,
            'card_id': row.card_id,
            'amount': row.amount,
            'merchant': row.merchant,
            'country': row.country,
            'timestamp': row.timestamp,
            'transaction_type': row.transaction_type,
            'risk_score': row.risk_score or 0.0,
            'fraud_label': row.fraud_label or False
        })

    df = pd.DataFrame(data)
    return df

def calculate_metrics(df):
    """
    Calculate dashboard metrics
    """
    total_transactions = len(df)
    fraud_transactions = df['fraud_label'].sum()
    fraud_rate = (fraud_transactions / total_transactions * 100) if total_transactions > 0 else 0
    avg_risk_score = df['risk_score'].mean()

    return {
        'total_transactions': total_transactions,
        'fraud_transactions': int(fraud_transactions),
        'fraud_rate': round(fraud_rate, 2),
        'avg_risk_score': round(avg_risk_score, 3)
    }

def create_charts(df):
    """
    Create dashboard charts
    """
    # Transactions over time
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['hour'] = df['timestamp'].dt.floor('H')

    time_chart = df.groupby('hour').size().reset_index(name='count')
    fig_time = px.line(time_chart, x='hour', y='count',
                      title='Transactions Over Time',
                      labels={'hour': 'Time', 'count': 'Number of Transactions'})

    # Fraud vs Legit
    fraud_counts = df['fraud_label'].value_counts().reset_index()
    fraud_counts.columns = ['Fraud', 'Count']
    fraud_counts['Fraud'] = fraud_counts['Fraud'].map({True: 'Fraud', False: 'Legit'})

    fig_fraud = px.bar(fraud_counts, x='Fraud', y='Count',
                      title='Fraud vs Legitimate Transactions',
                      color='Fraud', color_discrete_map={'Fraud': 'red', 'Legit': 'green'})

    # Risk score distribution
    fig_risk = px.histogram(df, x='risk_score', nbins=20,
                           title='Fraud Probability Distribution',
                           labels={'risk_score': 'Risk Score'})

    # Fraud by country
    country_fraud = df.groupby('country')['fraud_label'].agg(['count', 'sum']).reset_index()
    country_fraud['fraud_rate'] = (country_fraud['sum'] / country_fraud['count'] * 100).round(2)
    country_fraud = country_fraud.sort_values('fraud_rate', ascending=False)

    fig_country = px.bar(country_fraud, x='country', y='fraud_rate',
                        title='Fraud Rate by Country',
                        labels={'country': 'Country', 'fraud_rate': 'Fraud Rate (%)'})

    return fig_time, fig_fraud, fig_risk, fig_country

def main():
    st.title("🔍 Real-Time Fraud Detection Dashboard")

    # Auto-refresh
    if st.button("🔄 Refresh Data"):
        st.rerun()

    # Load data
    with st.spinner("Loading data..."):
        df = load_data()

    if df.empty:
        st.warning("No transaction data available. Please ensure the system is running.")
        return

    # Metrics
    metrics = calculate_metrics(df)

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Transactions", metrics['total_transactions'])

    with col2:
        st.metric("Fraud Transactions", metrics['fraud_transactions'])

    with col3:
        st.metric("Fraud Rate", f"{metrics['fraud_rate']}%")

    with col4:
        st.metric("Avg Risk Score", metrics['avg_risk_score'])

    # Charts
    st.header("Analytics")

    fig_time, fig_fraud, fig_risk, fig_country = create_charts(df)

    col1, col2 = st.columns(2)

    with col1:
        st.plotly_chart(fig_time, use_container_width=True)
        st.plotly_chart(fig_fraud, use_container_width=True)

    with col2:
        st.plotly_chart(fig_risk, use_container_width=True)
        st.plotly_chart(fig_country, use_container_width=True)

    # Transactions Table
    st.header("Recent Transactions")

    # Format dataframe for display
    display_df = df.copy()
    display_df['timestamp'] = display_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
    display_df['amount'] = display_df['amount'].round(2)
    display_df['risk_score'] = display_df['risk_score'].round(3)
    display_df['fraud_label'] = display_df['fraud_label'].map({True: '🚨 Fraud', False: '✅ Legit'})

    # Highlight fraudulent transactions
    def highlight_fraud(row):
        if row['fraud_label'] == '🚨 Fraud':
            return ['background-color: #ffcccc'] * len(row)
        return [''] * len(row)

    st.dataframe(
        display_df[['transaction_id', 'amount', 'merchant', 'country', 'timestamp', 'risk_score', 'fraud_label']],
        use_container_width=True
    )

    # Auto-refresh every 30 seconds
    time.sleep(30)
    st.rerun()

if __name__ == "__main__":
    main()
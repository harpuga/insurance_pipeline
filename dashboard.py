import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os

# Page configuration
st.set_page_config(
    page_title="Insurance Pipeline Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .stMetric {
        background-color: white;
        padding: 1rem;
        border-radius: 0.5rem;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    .stMetric > div > div > div {
        color: #262730 !important;
    }
    .stMetric > div > div > div[data-testid="metric-value"] {
        color: #262730 !important;
    }
    .stMetric > div > div > div[data-testid="metric-label"] {
        color: #262730 !important;
    }
    .stMetric label {
        color: #262730 !important;
    }
    .stMetric p {
        color: #262730 !important;
    }
    .stMetric span {
        color: #262730 !important;
    }
    h3 {
        color: #262730 !important;
    }
    .stMetric {
        color: #262730 !important;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data
def load_data():
    """Load data from DuckDB with caching for performance"""
    db_path = "data/db/warehouse.duckdb"
    
    if not os.path.exists(db_path):
        st.error(f"Database not found at {db_path}. Please run the pipeline first.")
        return None, None, None, None
    
    try:
        conn = duckdb.connect(database=db_path, read_only=True)
        
        # Load main mart table
        policy_metrics = conn.execute("SELECT * FROM f_policy_metrics").fetchdf()
        
        # Load staging data for additional analysis
        policies = conn.execute("SELECT * FROM stg_policies").fetchdf()
        payments = conn.execute("SELECT * FROM stg_payments").fetchdf()
        agents = conn.execute("SELECT * FROM stg_agents").fetchdf()
        
        conn.close()
        
        # Convert date columns
        if not policy_metrics.empty:
            policy_metrics['inception_date'] = pd.to_datetime(policy_metrics['inception_date'])
            policy_metrics['expiration_date'] = pd.to_datetime(policy_metrics['expiration_date'])
        
        if not payments.empty:
            payments['payment_date'] = pd.to_datetime(payments['payment_date'])
        
        return policy_metrics, policies, payments, agents
        
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        return None, None, None, None

def main():
    st.title("📊 Insurance Pipeline Dashboard")
    st.markdown("---")
    
    # Load data
    policy_metrics, policies, payments, agents = load_data()
    
    if policy_metrics is None:
        st.stop()
    
    # Sidebar filters
    st.sidebar.header("🔍 Filters")
    
    # Date range filter
    if not policy_metrics.empty:
        min_date = policy_metrics['inception_date'].min().date()
        max_date = policy_metrics['inception_date'].max().date()
        
        date_range = st.sidebar.date_input(
            "Policy Inception Date Range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date
        )
        
        # Agent filter
        agent_options = ['All'] + sorted(policy_metrics['agent_name'].dropna().unique().tolist())
        selected_agent = st.sidebar.selectbox("Select Agent", agent_options)
        
        # Status filter
        status_options = ['All'] + sorted(policy_metrics['status'].unique().tolist())
        selected_status = st.sidebar.selectbox("Policy Status", status_options)
        
        # Line of business filter
        lob_options = ['All'] + sorted(policy_metrics['line_of_business'].unique().tolist())
        selected_lob = st.sidebar.selectbox("Line of Business", lob_options)
        
        # Apply filters
        filtered_data = policy_metrics.copy()
        
        if len(date_range) == 2:
            filtered_data = filtered_data[
                (filtered_data['inception_date'].dt.date >= date_range[0]) &
                (filtered_data['inception_date'].dt.date <= date_range[1])
            ]
        
        if selected_agent != 'All':
            filtered_data = filtered_data[filtered_data['agent_name'] == selected_agent]
        
        if selected_status != 'All':
            filtered_data = filtered_data[filtered_data['status'] == selected_status]
        
        if selected_lob != 'All':
            filtered_data = filtered_data[filtered_data['line_of_business'] == selected_lob]
    
    # Main dashboard content
    if filtered_data.empty:
        st.warning("No data available for the selected filters.")
        return
    
    # Key Metrics Row
    st.subheader("📈 Key Performance Indicators")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_policies = len(filtered_data)
        st.metric("Total Policies", f"{total_policies:,}")
    
    with col2:
        total_written_premium = filtered_data['net_written_premium'].sum()
        st.metric("Total Written Premium", f"${total_written_premium:,.2f}")
    
    with col3:
        total_collected = filtered_data['collected_premium_total'].sum()
        st.metric("Total Collected Premium", f"${total_collected:,.2f}")
    
    with col4:
        collection_rate = (total_collected / total_written_premium * 100) if total_written_premium > 0 else 0
        st.metric("Collection Rate", f"{collection_rate:.1f}%")
    
    st.markdown("---")
    
    # Charts Row
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("💰 Premium by Agent")
        agent_premium = filtered_data.groupby('agent_name')['net_written_premium'].sum().reset_index()
        agent_premium = agent_premium.sort_values('net_written_premium', ascending=False).head(10)
        
        fig_agent = px.bar(
            agent_premium, 
            x='net_written_premium', 
            y='agent_name',
            orientation='h',
            title="Top 10 Agents by Written Premium",
            labels={'net_written_premium': 'Written Premium ($)', 'agent_name': 'Agent'}
        )
        fig_agent.update_layout(height=400)
        st.plotly_chart(fig_agent, use_container_width=True)
    
    with col2:
        st.subheader("📅 Premium Trends")
        if not filtered_data.empty:
            monthly_premium = filtered_data.groupby(
                filtered_data['inception_date'].dt.to_period('M')
            )['net_written_premium'].sum().reset_index()
            monthly_premium['inception_date'] = monthly_premium['inception_date'].astype(str)
            
            fig_trend = px.line(
                monthly_premium,
                x='inception_date',
                y='net_written_premium',
                title="Monthly Written Premium Trend",
                labels={'net_written_premium': 'Written Premium ($)', 'inception_date': 'Month'}
            )
            fig_trend.update_layout(height=400)
            st.plotly_chart(fig_trend, use_container_width=True)
    
    # Policy Status Distribution
    st.subheader("📊 Policy Status Distribution")
    col1, col2 = st.columns(2)
    
    with col1:
        status_counts = filtered_data['status'].value_counts()
        fig_status = px.pie(
            values=status_counts.values,
            names=status_counts.index,
            title="Policy Status Distribution"
        )
        st.plotly_chart(fig_status, use_container_width=True)
    
    with col2:
        st.subheader("🎯 Commission Analysis")
        commission_data = filtered_data.groupby('agent_name').agg({
            'net_written_commission_amount': 'sum',
            'collected_commission_amount': 'sum'
        }).reset_index()
        
        fig_commission = px.scatter(
            commission_data,
            x='net_written_commission_amount',
            y='collected_commission_amount',
            hover_data=['agent_name'],
            title="Written vs Collected Commission",
            labels={
                'net_written_commission_amount': 'Written Commission ($)',
                'collected_commission_amount': 'Collected Commission ($)'
            }
        )
        st.plotly_chart(fig_commission, use_container_width=True)
    
    # Data Table
    st.subheader("📋 Detailed Policy Data")
    
    # Select columns to display
    display_columns = [
        'policy_id', 'insured_name', 'line_of_business', 'status',
        'inception_date', 'expiration_date', 'net_written_premium',
        'collected_premium_total', 'agent_name', 'commission_rate_bps'
    ]
    
    available_columns = [col for col in display_columns if col in filtered_data.columns]
    display_data = filtered_data[available_columns]
    
    st.dataframe(
        display_data,
        use_container_width=True,
        height=400
    )
    
    # Download button
    csv = display_data.to_csv(index=False)
    st.download_button(
        label="📥 Download Filtered Data as CSV",
        data=csv,
        file_name=f"insurance_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv"
    )

if __name__ == "__main__":
    main()

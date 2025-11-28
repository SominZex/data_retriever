import streamlit as st
import psycopg2
import polars as pl
import pandas as pd
from datetime import datetime, timedelta
from psycopg2 import pool
import plotly.express as px
import plotly.graph_objects as go

# Page config
st.set_page_config(page_title="Sales Analytics Dashboard", page_icon="📊", layout="wide")

# Title
st.title("📊 Store Sales Analytics Dashboard")

# Database connection pool
@st.cache_resource
def get_connection_pool():
    return pool.SimpleConnectionPool(
        1, 10,
        host='ip_address',
        port='port_no',
        dbname='db_name',
        user='user_name',
        password='user_pw',
        connect_timeout=10
    )

# Load store list
@st.cache_data(ttl=7200)
def load_stores():
    """Load all available stores"""
    conn_pool = get_connection_pool()
    conn = conn_pool.getconn()
    
    try:
        cur = conn.cursor()
        cur.execute('SELECT DISTINCT "storeName" FROM billing_data WHERE "storeName" IS NOT NULL ORDER BY "storeName"')
        stores = [row[0] for row in cur.fetchall()]
        cur.close()
        return stores
    except Exception as e:
        st.error(f"Error loading stores: {str(e)}")
        return []
    finally:
        conn_pool.putconn(conn)

# Fetch sales data
def fetch_sales_data(start_date, end_date, store_name=None):
    """Fetch sales data for the given date range and optional store"""
    conn_pool = get_connection_pool()
    conn = conn_pool.getconn()
    
    try:
        cur = conn.cursor()
        
        # Build query
        where_conditions = ['"orderDate" BETWEEN %s AND %s']
        params = [start_date, end_date]
        
        if store_name:
            where_conditions.append('"storeName" = %s')
            params.append(store_name)
        
        where_clause = " AND ".join(where_conditions)
        
        # Query to get daily sales with store grouping
        query = f'''
            SELECT 
                "orderDate",
                "storeName",
                SUM("totalProductPrice") as daily_sales,
                COUNT(*) as transaction_count
            FROM billing_data 
            WHERE {where_clause}
            GROUP BY "orderDate", "storeName"
            ORDER BY "orderDate", "storeName"
        '''
        
        cur.execute(query, params)
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        cur.close()
        
        if not rows:
            return None
        
        # Convert to Polars DataFrame
        df = pl.DataFrame(rows, schema=columns, orient="row")
        return df
        
    except Exception as e:
        st.error(f"Error fetching data: {str(e)}")
        return None
    finally:
        conn_pool.putconn(conn)

# Calculate metrics
def calculate_metrics(df, start_date, end_date):
    """Calculate all sales metrics from the dataframe"""
    if df is None or len(df) == 0:
        return None
    
    # Convert to pandas for easier calculations
    pdf = df.to_pandas()
    pdf['orderDate'] = pd.to_datetime(pdf['orderDate'])
    
    # Total sales
    total_sales = pdf['daily_sales'].sum()
    
    # Date range calculations
    date_range = (end_date - start_date).days + 1  # +1 to include both start and end dates
    
    # Average sales per day (total sales / number of days in range)
    avg_sales_per_day = total_sales / date_range if date_range > 0 else 0
    
    # Weekly calculations
    # Group by week
    pdf['week'] = pdf['orderDate'].dt.to_period('W')
    weekly_sales = pdf.groupby('week')['daily_sales'].sum()
    
    total_weeks = len(weekly_sales)
    weekly_total_sales = weekly_sales.sum()
    weekly_avg_sales = weekly_sales.mean() if total_weeks > 0 else 0
    
    # Monthly calculations
    pdf['month'] = pdf['orderDate'].dt.to_period('M')
    monthly_sales = pdf.groupby('month')['daily_sales'].sum()
    
    total_months = len(monthly_sales)
    monthly_avg_sales = monthly_sales.mean() if total_months > 0 else 0
    
    # Total transactions
    total_transactions = pdf['transaction_count'].sum()
    
    # Store-wise breakdown (if multiple stores)
    store_breakdown = pdf.groupby('storeName').agg({
        'daily_sales': 'sum',
        'transaction_count': 'sum'
    }).reset_index()
    store_breakdown.columns = ['storeName', 'total_sales', 'total_transactions']
    store_breakdown = store_breakdown.sort_values('total_sales', ascending=False)
    
    return {
        'total_sales': total_sales,
        'avg_sales_per_day': avg_sales_per_day,
        'weekly_total_sales': weekly_total_sales,
        'weekly_avg_sales': weekly_avg_sales,
        'monthly_avg_sales': monthly_avg_sales,
        'total_transactions': total_transactions,
        'date_range_days': date_range,
        'total_weeks': total_weeks,
        'total_months': total_months,
        'store_breakdown': store_breakdown,
        'daily_data': pdf,
        'weekly_data': weekly_sales,
        'monthly_data': monthly_sales
    }

# Main app
try:
    # Filters Section
    st.subheader("🔍 Filters")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        start_date = st.date_input(
            "Start Date", 
            value=datetime.now() - timedelta(days=30),
            max_value=datetime.now()
        )
    
    with col2:
        end_date = st.date_input(
            "End Date", 
            value=datetime.now(),
            max_value=datetime.now()
        )
    
    with col3:
        # Load stores
        with st.spinner("Loading stores..."):
            stores = load_stores()
        
        store_options = ["-- All Stores --"] + stores
        selected_store = st.selectbox(
            f"🏪 Store Name ({len(stores)} available)",
            options=store_options,
            index=0
        )
    
    # Convert store selection
    store_filter = None if selected_store == "-- All Stores --" else selected_store
    
    # Validate dates
    if start_date > end_date:
        st.error("⚠️ Start date must be before or equal to end date!")
        st.stop()
    
    st.divider()
    
    # Fetch and analyze data
    with st.spinner("Fetching sales data..."):
        df = fetch_sales_data(start_date, end_date, store_filter)
    
    if df is None or len(df) == 0:
        st.warning("⚠️ No data found for the selected filters")
        st.stop()
    
    # Calculate metrics
    metrics = calculate_metrics(df, start_date, end_date)
    
    if metrics is None:
        st.error("Error calculating metrics")
        st.stop()
    
    # Display Key Metrics
    st.subheader("📈 Key Metrics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Total Sales",
            value=f"₹{metrics['total_sales']:,.2f}",
            help=f"Total sales from {start_date} to {end_date}"
        )
    
    with col2:
        st.metric(
            label="Avg Sales/Day",
            value=f"₹{metrics['avg_sales_per_day']:,.2f}",
            help=f"Average sales per day over {metrics['date_range_days']} days"
        )
    
    with col3:
        st.metric(
            label="Weekly Avg Sales",
            value=f"₹{metrics['weekly_avg_sales']:,.2f}",
            help=f"Average weekly sales across {metrics['total_weeks']} week(s)"
        )
    
    with col4:
        st.metric(
            label="Monthly Avg Sales",
            value=f"₹{metrics['monthly_avg_sales']:,.2f}",
            help=f"Average monthly sales across {metrics['total_months']} month(s)"
        )
    
    # Second row of metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Total Transactions",
            value=f"{int(metrics['total_transactions']):,}",
            help="Total number of transactions"
        )
    
    with col2:
        st.metric(
            label="Date Range",
            value=f"{metrics['date_range_days']} days",
            help="Number of days in selected range"
        )
    
    with col3:
        st.metric(
            label="Total Weeks",
            value=f"{metrics['total_weeks']}",
            help="Number of weeks in the date range"
        )
    
    with col4:
        avg_transaction_value = metrics['total_sales'] / metrics['total_transactions'] if metrics['total_transactions'] > 0 else 0
        st.metric(
            label="Avg Transaction",
            value=f"₹{avg_transaction_value:,.2f}",
            help="Average value per transaction"
        )
    
    st.divider()
    
    # Store-wise Breakdown (if all stores selected)
    if store_filter is None and len(metrics['store_breakdown']) > 1:
        st.subheader("🏪 Store-wise Sales Breakdown")
        
        # Top stores chart
        fig_stores = px.bar(
            metrics['store_breakdown'].head(10),
            x='storeName',
            y='total_sales',
            title='Top 10 Stores by Total Sales',
            labels={'total_sales': 'Total Sales (₹)', 'storeName': 'Store Name'},
            color='total_sales',
            color_continuous_scale='Blues'
        )
        fig_stores.update_layout(height=400, showlegend=False)
        st.plotly_chart(fig_stores, use_container_width=True)
        
        # Store breakdown table
        with st.expander("📋 View All Stores"):
            display_df = metrics['store_breakdown'].copy()
            display_df['total_sales'] = display_df['total_sales'].apply(lambda x: f"₹{x:,.2f}")
            display_df['total_transactions'] = display_df['total_transactions'].apply(lambda x: f"{int(x):,}")
            display_df.columns = ['Store Name', 'Total Sales', 'Total Transactions']
            st.dataframe(display_df, use_container_width=True, height=400)
        
        st.divider()
    
    # Daily Sales Trend
    st.subheader("📅 Daily Sales Trend")
    
    # Prepare data for daily chart
    daily_chart_data = metrics['daily_data'].groupby('orderDate')['daily_sales'].sum().reset_index()
    
    fig_daily = go.Figure()
    
    fig_daily.add_trace(go.Scatter(
        x=daily_chart_data['orderDate'],
        y=daily_chart_data['daily_sales'],
        mode='lines+markers',
        name='Daily Sales',
        line=dict(color='#1f77b4', width=2),
        marker=dict(size=6)
    ))
    
    # Add average line
    fig_daily.add_hline(
        y=metrics['avg_sales_per_day'],
        line_dash="dash",
        line_color="red",
        annotation_text=f"Avg: ₹{metrics['avg_sales_per_day']:,.0f}",
        annotation_position="right"
    )
    
    fig_daily.update_layout(
        title='Daily Sales Over Time',
        xaxis_title='Date',
        yaxis_title='Sales (₹)',
        height=400,
        hovermode='x unified'
    )
    
    st.plotly_chart(fig_daily, use_container_width=True)
    
    st.divider()
    
    # Weekly Analysis
    st.subheader("📊 Weekly Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.metric(
            label="Total Weekly Sales",
            value=f"₹{metrics['weekly_total_sales']:,.2f}",
            help="Sum of all weekly sales"
        )
    
    with col2:
        st.metric(
            label="Average Weekly Sales",
            value=f"₹{metrics['weekly_avg_sales']:,.2f}",
            help=f"Average across {metrics['total_weeks']} week(s)"
        )
    
    # Weekly chart
    weekly_chart_data = pd.DataFrame({
        'Week': [str(w) for w in metrics['weekly_data'].index],
        'Sales': metrics['weekly_data'].values
    })
    
    fig_weekly = px.bar(
        weekly_chart_data,
        x='Week',
        y='Sales',
        title='Weekly Sales Distribution',
        labels={'Sales': 'Total Sales (₹)', 'Week': 'Week'},
        color='Sales',
        color_continuous_scale='Greens'
    )
    fig_weekly.update_layout(height=350, showlegend=False)
    st.plotly_chart(fig_weekly, use_container_width=True)
    
    st.divider()
    
    # Monthly Analysis
    if metrics['total_months'] > 0:
        st.subheader("📆 Monthly Analysis")
        
        st.metric(
            label="Average Monthly Sales",
            value=f"₹{metrics['monthly_avg_sales']:,.2f}",
            help=f"Average across {metrics['total_months']} month(s)"
        )
        
        # Monthly chart
        monthly_chart_data = pd.DataFrame({
            'Month': [str(m) for m in metrics['monthly_data'].index],
            'Sales': metrics['monthly_data'].values
        })
        
        fig_monthly = px.bar(
            monthly_chart_data,
            x='Month',
            y='Sales',
            title='Monthly Sales Distribution',
            labels={'Sales': 'Total Sales (₹)', 'Month': 'Month'},
            color='Sales',
            color_continuous_scale='Purples'
        )
        fig_monthly.update_layout(height=350, showlegend=False)
        st.plotly_chart(fig_monthly, use_container_width=True)
        
        st.divider()
    
    # Raw Data Table
    with st.expander("📋 View Raw Daily Data"):
        display_daily = metrics['daily_data'][['orderDate', 'storeName', 'daily_sales', 'transaction_count']].copy()
        display_daily['daily_sales'] = display_daily['daily_sales'].apply(lambda x: f"₹{x:,.2f}")
        display_daily['transaction_count'] = display_daily['transaction_count'].apply(lambda x: int(x))
        display_daily.columns = ['Date', 'Store', 'Daily Sales', 'Transactions']
        st.dataframe(display_daily, use_container_width=True, height=400)
    
    # Export option
    st.divider()
    
    if st.button("📥 Export Summary Report", type="primary"):
        # Create summary report
        summary_data = {
            'Metric': [
                'Date Range',
                'Total Days',
                'Total Sales',
                'Average Sales per Day',
                'Weekly Average Sales',
                'Monthly Average Sales',
                'Total Transactions',
                'Average Transaction Value',
                'Total Weeks',
                'Total Months'
            ],
            'Value': [
                f"{start_date} to {end_date}",
                f"{metrics['date_range_days']} days",
                f"₹{metrics['total_sales']:,.2f}",
                f"₹{metrics['avg_sales_per_day']:,.2f}",
                f"₹{metrics['weekly_avg_sales']:,.2f}",
                f"₹{metrics['monthly_avg_sales']:,.2f}",
                f"{int(metrics['total_transactions']):,}",
                f"₹{avg_transaction_value:,.2f}",
                f"{metrics['total_weeks']}",
                f"{metrics['total_months']}"
            ]
        }
        
        summary_df = pd.DataFrame(summary_data)
        csv = summary_df.to_csv(index=False)
        
        st.download_button(
            label="💾 Download Summary CSV",
            data=csv,
            file_name=f"sales_summary_{start_date}_to_{end_date}.csv",
            mime="text/csv"
        )

except Exception as e:
    st.error("⚠️ An error occurred")
    st.error(str(e))
    
    if st.button("🔄 Retry"):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.rerun()

# Sidebar
with st.sidebar:
    st.header("ℹ️ About")
    st.markdown("""
    This dashboard provides comprehensive sales analytics including:
    
    - **Daily Sales Trends**
    - **Weekly Analysis**
    - **Monthly Averages**
    - **Store-wise Breakdown**
    - **Transaction Metrics**
    """)
    
    st.divider()
    
    st.header("📊 Metrics Explained")
    st.markdown("""
    **Average Sales/Day**: Total sales divided by the number of days in the selected date range.
    
    **Weekly Avg Sales**: Average of all weekly totals within the date range.
    
    **Monthly Avg Sales**: Average of all monthly totals within the date range.
    
    **Total Transactions**: Count of all billing records in the selected period.
    """)
    
    st.divider()
    
    if st.button("🗑️ Clear Cache"):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.success("Cache cleared!")
        st.rerun()
import streamlit as st
import psycopg2
import polars as pl
from datetime import datetime, timedelta
from psycopg2 import pool
import hashlib

# Page config
st.set_page_config(page_title="CSV Data Downloader", page_icon="📊", layout="wide")

# Title
st.title("📊 Export Data to CSV")

# Database connection pool
@st.cache_resource
def get_connection_pool():
    return pool.SimpleConnectionPool(
        1, 10,
        host=st.secrets["DB_HOST"],
        port=st.secrets["DB_PORT"],
        dbname=st.secrets["DB_NAME"],
        user=st.secrets["DB_USER"],
        password=st.secrets["DB_PASS"],
        connect_timeout=10
    )

# Cascading filter loader with smart caching
@st.cache_data(ttl=7200)
def load_cascading_filters(brand=None, category=None, subcategory=None, store=None):
    """Load filters based on current selections - cascading effect"""
    conn_pool = get_connection_pool()
    conn = conn_pool.getconn()
    
    try:
        cur = conn.cursor()
        
        # Build WHERE clause based on current selections
        where_conditions = []
        params = []
        
        if brand:
            where_conditions.append('"brandName" = %s')
            params.append(brand)
        if category:
            where_conditions.append('"categoryName" = %s')
            params.append(category)
        if subcategory:
            where_conditions.append('"subCategoryOf" = %s')
            params.append(subcategory)
        if store:
            where_conditions.append('"storeName" = %s')
            params.append(store)
        
        where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
        
        # Try materialized view first, fallback to main table
        try:
            table_name = "filter_lookup"
            cur.execute(f'SELECT 1 FROM {table_name} LIMIT 1')
        except:
            table_name = "billing_data"
        
        # Load each filter with current constraints
        # Brands
        brand_query = f'''
            SELECT DISTINCT "brandName" 
            FROM {table_name} 
            WHERE "brandName" IS NOT NULL 
            {f"AND {where_clause.replace('"brandName" = %s', '1=1')}" if brand else f"AND {where_clause}"}
            ORDER BY "brandName" 
        '''
        brand_params = [p for i, p in enumerate(params) if not (brand and i == 0)]
        cur.execute(brand_query, brand_params)
        brands = [row[0] for row in cur.fetchall()]
        
        # Categories
        cat_query = f'''
            SELECT DISTINCT "categoryName" 
            FROM {table_name} 
            WHERE "categoryName" IS NOT NULL 
            {f"AND {where_clause.replace('"categoryName" = %s', '1=1')}" if category else f"AND {where_clause}"}
            ORDER BY "categoryName" 
            LIMIT 1000
        '''
        cat_params = [p for i, p in enumerate(params) if not (category and params[i] == category)]
        cur.execute(cat_query, cat_params)
        categories = [row[0] for row in cur.fetchall()]
        
        # Subcategories
        subcat_query = f'''
            SELECT DISTINCT "subCategoryOf" 
            FROM {table_name} 
            WHERE "subCategoryOf" IS NOT NULL 
            {f"AND {where_clause.replace('"subCategoryOf" = %s', '1=1')}" if subcategory else f"AND {where_clause}"}
            ORDER BY "subCategoryOf" 
            LIMIT 1000
        '''
        subcat_params = [p for i, p in enumerate(params) if not (subcategory and params[i] == subcategory)]
        cur.execute(subcat_query, subcat_params)
        subcategories = [row[0] for row in cur.fetchall()]
        
        # Stores
        store_query = f'''
            SELECT DISTINCT "storeName" 
            FROM {table_name} 
            WHERE "storeName" IS NOT NULL 
            {f"AND {where_clause.replace('"storeName" = %s', '1=1')}" if store else f"AND {where_clause}"}
            ORDER BY "storeName" 
            LIMIT 1000
        '''
        store_params = [p for i, p in enumerate(params) if not (store and params[i] == store)]
        cur.execute(store_query, store_params)
        stores = [row[0] for row in cur.fetchall()]
        
        # FIXED: Store results before closing cursor
        result = (brands, categories, subcategories, stores)
        cur.close()
        return result
        
    except Exception as e:
        st.error(f"Filter loading error: {str(e)}")
        return [], [], [], []
    finally:
        conn_pool.putconn(conn)

# Load unavailable filters - optimized like cascading filters
@st.cache_data(ttl=7200)
def load_unavailable_filters(start_date, end_date, brand=None, category=None, subcategory=None, store=None):
    """Load filters that are unavailable based on current selections - using filter_lookup for speed"""
    conn_pool = get_connection_pool()
    conn = conn_pool.getconn()
    
    try:
        cur = conn.cursor()
        
        # Build WHERE clause for available items
        where_conditions = ['"orderDate" BETWEEN %s AND %s']
        params = [start_date, end_date]
        
        if brand:
            where_conditions.append('"brandName" = %s')
            params.append(brand)
        if category:
            where_conditions.append('"categoryName" = %s')
            params.append(category)
        if subcategory:
            where_conditions.append('"subCategoryOf" = %s')
            params.append(subcategory)
        if store:
            where_conditions.append('"storeName" = %s')
            params.append(store)
        
        where_clause = " AND ".join(where_conditions)
        
        # Try materialized view first, fallback to main table
        try:
            table_name = "filter_lookup"
            cur.execute(f'SELECT 1 FROM {table_name} LIMIT 1')
        except:
            table_name = "billing_data"
        
        # Get all distinct values from filter_lookup (fast)
        cur.execute(f'SELECT DISTINCT "brandName" FROM {table_name} WHERE "brandName" IS NOT NULL ORDER BY "brandName"')
        all_brands = set(row[0] for row in cur.fetchall())
        
        cur.execute(f'SELECT DISTINCT "categoryName" FROM {table_name} WHERE "categoryName" IS NOT NULL ORDER BY "categoryName"')
        all_categories = set(row[0] for row in cur.fetchall())
        
        cur.execute(f'SELECT DISTINCT "subCategoryOf" FROM {table_name} WHERE "subCategoryOf" IS NOT NULL ORDER BY "subCategoryOf"')
        all_subcategories = set(row[0] for row in cur.fetchall())
        
        cur.execute(f'SELECT DISTINCT "storeName" FROM {table_name} WHERE "storeName" IS NOT NULL ORDER BY "storeName"')
        all_stores = set(row[0] for row in cur.fetchall())
        
        # Get available values based on current filters (from billing_data for date filtering)
        cur.execute(f'SELECT DISTINCT "brandName" FROM billing_data WHERE {where_clause} AND "brandName" IS NOT NULL', params)
        available_brands = set(row[0] for row in cur.fetchall())
        
        cur.execute(f'SELECT DISTINCT "categoryName" FROM billing_data WHERE {where_clause} AND "categoryName" IS NOT NULL', params)
        available_categories = set(row[0] for row in cur.fetchall())
        
        cur.execute(f'SELECT DISTINCT "subCategoryOf" FROM billing_data WHERE {where_clause} AND "subCategoryOf" IS NOT NULL', params)
        available_subcategories = set(row[0] for row in cur.fetchall())
        
        cur.execute(f'SELECT DISTINCT "storeName" FROM billing_data WHERE {where_clause} AND "storeName" IS NOT NULL', params)
        available_stores = set(row[0] for row in cur.fetchall())
        
        # Calculate unavailable items
        unavailable_brands = sorted(all_brands - available_brands)
        unavailable_categories = sorted(all_categories - available_categories)
        unavailable_subcategories = sorted(all_subcategories - available_subcategories)
        unavailable_stores = sorted(all_stores - available_stores)
        
        # FIXED: Store results before closing cursor
        result = (unavailable_brands, unavailable_categories, unavailable_subcategories, unavailable_stores)
        cur.close()
        return result
        
    except Exception as e:
        st.error(f"Error loading unavailable filters: {str(e)}")
        return [], [], [], []
    finally:
        conn_pool.putconn(conn)

# Initialize session state
if 'brand' not in st.session_state:
    st.session_state.brand = None
if 'category' not in st.session_state:
    st.session_state.category = None
if 'subcategory' not in st.session_state:
    st.session_state.subcategory = None
if 'store' not in st.session_state:
    st.session_state.store = None
if 'filters_changed' not in st.session_state:
    st.session_state.filters_changed = False

try:
    # Date Range (Mandatory)
    st.subheader("📅 Date Range (Required)")
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Start Date", value=datetime.now() - timedelta(days=7))
    with col2:
        end_date = st.date_input("End Date", value=datetime.now())
    
    st.divider()
    
    # Optional Filters with cascading
    st.subheader("🔍 Cascading Filters")
    st.caption("Filters update dynamically based on your selections")
    
    # Load cascading filters based on current state
    with st.spinner("Loading available options..."):
        brands, categories, subcategories, stores = load_cascading_filters(
            st.session_state.brand,
            st.session_state.category,
            st.session_state.subcategory,
            st.session_state.store
        )
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Brand filter
        brand_options = [None] + brands
        brand_index = brand_options.index(st.session_state.brand) if st.session_state.brand in brand_options else 0
        
        new_brand = st.selectbox(
            f"🏷️ Brand Name ({len(brands)} available)",
            options=brand_options,
            index=brand_index,
            format_func=lambda x: "-- All Brands --" if x is None else x,
            key="brand_select",
            help="Select a brand to filter other options"
        )
        
        if new_brand != st.session_state.brand:
            st.session_state.brand = new_brand
            st.session_state.filters_changed = True
            st.rerun()
        
        # Category filter
        category_options = [None] + categories
        category_index = category_options.index(st.session_state.category) if st.session_state.category in category_options else 0
        
        new_category = st.selectbox(
            f"📂 Category Name ({len(categories)} available)",
            options=category_options,
            index=category_index,
            format_func=lambda x: "-- All Categories --" if x is None else x,
            key="category_select",
            help="Select a category to filter other options"
        )
        
        if new_category != st.session_state.category:
            st.session_state.category = new_category
            st.session_state.filters_changed = True
            st.rerun()
    
    with col2:
        # Subcategory filter
        subcategory_options = [None] + subcategories
        subcategory_index = subcategory_options.index(st.session_state.subcategory) if st.session_state.subcategory in subcategory_options else 0
        
        new_subcategory = st.selectbox(
            f"📑 Sub Category ({len(subcategories)} available)",
            options=subcategory_options,
            index=subcategory_index,
            format_func=lambda x: "-- All Sub Categories --" if x is None else x,
            key="subcategory_select",
            help="Select a subcategory to filter other options"
        )
        
        if new_subcategory != st.session_state.subcategory:
            st.session_state.subcategory = new_subcategory
            st.session_state.filters_changed = True
            st.rerun()
        
        # Store filter
        store_options = [None] + stores
        store_index = store_options.index(st.session_state.store) if st.session_state.store in store_options else 0
        
        new_store = st.selectbox(
            f"🏪 Store Name ({len(stores)} available)",
            options=store_options,
            index=store_index,
            format_func=lambda x: "-- All Stores --" if x is None else x,
            key="store_select",
            help="Select a store to filter other options"
        )
        
        if new_store != st.session_state.store:
            st.session_state.store = new_store
            st.session_state.filters_changed = True
            st.rerun()
    
    # Clear filters button
    if st.button("🔄 Clear All Filters"):
        st.session_state.brand = None
        st.session_state.category = None
        st.session_state.subcategory = None
        st.session_state.store = None
        st.session_state.filters_changed = False
        st.rerun()
    
    st.divider()
    
    # Download button
    if st.button("📥 Generate & Download CSV", type="primary", use_container_width=True):
        if not start_date or not end_date:
            st.error("Please select both start and end dates")
        else:
            with st.spinner("Generating CSV..."):
                try:
                    conn_pool = get_connection_pool()
                    conn = conn_pool.getconn()
                    
                    try:
                        # Build WHERE clause
                        where_conditions = ['"orderDate" BETWEEN %s AND %s']
                        params = [start_date, end_date]
                        
                        if st.session_state.brand:
                            where_conditions.append('"brandName" = %s')
                            params.append(st.session_state.brand)
                        
                        if st.session_state.category:
                            where_conditions.append('"categoryName" = %s')
                            params.append(st.session_state.category)
                        
                        if st.session_state.subcategory:
                            where_conditions.append('"subCategoryOf" = %s')
                            params.append(st.session_state.subcategory)
                        
                        if st.session_state.store:
                            where_conditions.append('"storeName" = %s')
                            params.append(st.session_state.store)
                        
                        where_clause = " AND ".join(where_conditions)
                        
                        # Count query (no ORDER BY)
                        count_query = f'SELECT COUNT(*) FROM billing_data WHERE {where_clause}'
                        
                        # Main query (with ORDER BY)
                        main_query = f'SELECT * FROM billing_data WHERE {where_clause} ORDER BY "orderDate" DESC'
                        
                        # Get count with timeout protection
                        cur = conn.cursor()
                        cur.execute("SET statement_timeout = '30s'")  # 30 second timeout
                        cur.execute(count_query, params)
                        total_rows = cur.fetchone()[0]
                        cur.close()
                        
                        if total_rows == 0:
                            st.warning("⚠️ No data found for the selected filters")
                        elif total_rows > 1000000:
                            st.error(f"⚠️ Dataset too large: {total_rows:,} rows. Please narrow your filters.")
                        elif total_rows > 500000:
                            st.warning(f"⚠️ Large dataset: {total_rows:,} rows. This may take a while...")
                        
                        if 0 < total_rows <= 1000000:
                            progress_bar = st.progress(0, text="Fetching data...")
                            
                            # Execute main query with timeout
                            cur = conn.cursor()
                            cur.execute("SET statement_timeout = '300s'")
                            cur.execute(main_query, params)
                            
                            columns = [desc[0] for desc in cur.description]
                            
                            # Fetch in batches
                            batch_size = 50000
                            all_rows = []
                            
                            while True:
                                rows = cur.fetchmany(batch_size)
                                if not rows:
                                    break
                                all_rows.extend(rows)
                                progress = min(len(all_rows) / total_rows, 0.9)
                                progress_bar.progress(progress, text=f"Fetching data... {len(all_rows):,}/{total_rows:,} rows")
                            
                            cur.close()
                            
                            progress_bar.progress(0.95, text="Converting to CSV...")
                            
                            # Create CSV
                            df = pl.DataFrame(all_rows, schema=columns, orient="row")
                            csv = df.write_csv()
                            
                            progress_bar.progress(1.0, text="Done!")
                            progress_bar.empty()
                            
                            filename = f"billing_data_{start_date}_to_{end_date}.csv"
                            
                            st.success(f"✅ Generated CSV with {len(df):,} records!")
                            
                            # Download button
                            st.download_button(
                                label="💾 Download CSV File",
                                data=csv,
                                file_name=filename,
                                mime="text/csv",
                                use_container_width=True
                            )
                            
                            # Show preview
                            with st.expander("👁️ Preview (first 10 rows)"):
                                st.dataframe(df.head(10).to_pandas(), use_container_width=True)
                    
                    finally:
                        conn_pool.putconn(conn)
                
                except psycopg2.OperationalError as e:
                    if "timeout" in str(e).lower():
                        st.error("⏱️ Query timeout! Please narrow your date range or add more filters.")
                    else:
                        st.error(f"Database connection error: {str(e)}")
                        st.info("💡 Try: 1) Narrowing date range, 2) Adding more filters, 3) Retrying in a moment")
                except Exception as e:
                    st.error(f"Error: {str(e)}")
                    import traceback
                    st.code(traceback.format_exc())
    
    # Show active filters
    with st.expander("📋 Active Filters Summary"):
        st.write(f"**Date Range:** {start_date} to {end_date}")
        active_filters = []
        if st.session_state.brand:
            active_filters.append(f"**Brand:** {st.session_state.brand}")
        if st.session_state.category:
            active_filters.append(f"**Category:** {st.session_state.category}")
        if st.session_state.subcategory:
            active_filters.append(f"**Sub Category:** {st.session_state.subcategory}")
        if st.session_state.store:
            active_filters.append(f"**Store:** {st.session_state.store}")
        
        if active_filters:
            for f in active_filters:
                st.write(f)
        else:
            st.write("*No filters applied - will export all data in date range*")
    
    st.divider()
    
    # === NEW SECTION: Unavailable Filters ===
    st.header("🚫 Unavailable Filters")
    st.caption("These values are not available for your current filter selection")
    
    with st.spinner("Loading unavailable filters..."):
        unavail_brands, unavail_categories, unavail_subcategories, unavail_stores = load_unavailable_filters(
            start_date, end_date,
            st.session_state.brand,
            st.session_state.category,
            st.session_state.subcategory,
            st.session_state.store
        )
    
    # Display unavailable brands
    st.subheader(f"🏷️ Unavailable Brands ({len(unavail_brands)})")
    if unavail_brands:
        df_unavail_brands = pl.DataFrame({"Brand Name": unavail_brands})
        st.dataframe(df_unavail_brands.to_pandas(), use_container_width=True, height=min(len(unavail_brands) * 35 + 38, 400))
        
        csv_brands = df_unavail_brands.write_csv()
        st.download_button(
            label=f"📥 Download Unavailable Brands ({len(unavail_brands)})",
            data=csv_brands,
            file_name=f"unavailable_brands_{start_date}_to_{end_date}.csv",
            mime="text/csv",
            key="download_unavail_brands"
        )
    else:
        st.info("✅ All brands are available for your current selection")
    
    st.divider()
    
    # Display unavailable categories
    st.subheader(f"📂 Unavailable Categories ({len(unavail_categories)})")
    if unavail_categories:
        df_unavail_categories = pl.DataFrame({"Category Name": unavail_categories})
        st.dataframe(df_unavail_categories.to_pandas(), use_container_width=True, height=min(len(unavail_categories) * 35 + 38, 400))
        
        csv_categories = df_unavail_categories.write_csv()
        st.download_button(
            label=f"📥 Download Unavailable Categories ({len(unavail_categories)})",
            data=csv_categories,
            file_name=f"unavailable_categories_{start_date}_to_{end_date}.csv",
            mime="text/csv",
            key="download_unavail_categories"
        )
    else:
        st.info("✅ All categories are available for your current selection")
    
    st.divider()
    
    # Display unavailable subcategories
    st.subheader(f"📑 Unavailable Sub Categories ({len(unavail_subcategories)})")
    if unavail_subcategories:
        df_unavail_subcategories = pl.DataFrame({"Sub Category": unavail_subcategories})
        st.dataframe(df_unavail_subcategories.to_pandas(), use_container_width=True, height=min(len(unavail_subcategories) * 35 + 38, 400))
        
        csv_subcategories = df_unavail_subcategories.write_csv()
        st.download_button(
            label=f"📥 Download Unavailable Sub Categories ({len(unavail_subcategories)})",
            data=csv_subcategories,
            file_name=f"unavailable_subcategories_{start_date}_to_{end_date}.csv",
            mime="text/csv",
            key="download_unavail_subcategories"
        )
    else:
        st.info("✅ All sub categories are available for your current selection")
    
    st.divider()
    
    # Display unavailable stores
    st.subheader(f"🏪 Unavailable Stores ({len(unavail_stores)})")
    if unavail_stores:
        df_unavail_stores = pl.DataFrame({"Store Name": unavail_stores})
        st.dataframe(df_unavail_stores.to_pandas(), use_container_width=True, height=min(len(unavail_stores) * 35 + 38, 400))
        
        csv_stores = df_unavail_stores.write_csv()
        st.download_button(
            label=f"📥 Download Unavailable Stores ({len(unavail_stores)})",
            data=csv_stores,
            file_name=f"unavailable_stores_{start_date}_to_{end_date}.csv",
            mime="text/csv",
            key="download_unavail_stores"
        )
    else:
        st.info("✅ All stores are available for your current selection")

except Exception as e:
    st.error("⚠️ Database Connection Error")
    st.error(str(e))
    
    if "timeout" in str(e).lower():
        st.warning("The query took too long. Try narrowing your date range or adding filters.")
    elif "recovery mode" in str(e).lower():
        st.warning("The database is in recovery mode. Please try again in a few minutes.")
    elif "closed the connection" in str(e).lower():
        st.warning("Database connection lost. The server may be overloaded or restarting.")
        st.info("💡 **Suggestions:**\n- Wait a moment and retry\n- Use narrower date ranges\n- Apply more filters to reduce dataset size")
    
    if st.button("🔄 Retry Connection"):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.rerun()

# Sidebar with setup instructions
with st.sidebar:
    st.header("⚡ Performance Tips")
    
    st.markdown("""
    **For Best Performance:**
    1. ✅ Use date ranges < 3 months
    2. ✅ Apply at least one filter
    3. ✅ Start with brand/store filter
    4. ❌ Avoid very wide date ranges
    """)
    
    st.divider()
    
    st.header("🔧 Database Setup")
    st.markdown("""
    **Run this SQL once for faster filters:**
    
    ```sql
    CREATE MATERIALIZED VIEW filter_lookup AS
    SELECT DISTINCT 
        "brandName", 
        "categoryName", 
        "subCategoryOf", 
        "storeName"
    FROM billing_data 
    WHERE "brandName" IS NOT NULL;
    
    CREATE INDEX idx_filter_brand 
    ON filter_lookup("brandName");
    
    CREATE INDEX idx_filter_category 
    ON filter_lookup("categoryName");
    
    CREATE INDEX idx_filter_subcategory 
    ON filter_lookup("subCategoryOf");
    
    CREATE INDEX idx_filter_store 
    ON filter_lookup("storeName");
    
    -- Refresh daily
    REFRESH MATERIALIZED VIEW filter_lookup;
    ```
    
    **Add indexes to main table:**
    ```sql
    CREATE INDEX idx_billing_date 
    ON billing_data("orderDate");
    
    CREATE INDEX idx_billing_brand 
    ON billing_data("brandName");
    
    CREATE INDEX idx_billing_store 
    ON billing_data("storeName");
    ```
    """)
    
    st.divider()
    
    if st.button("🗑️ Clear Cache & Reload"):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.success("Cache cleared!")
        st.rerun()
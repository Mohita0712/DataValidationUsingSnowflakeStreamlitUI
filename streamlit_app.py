# Import python packages
import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

session = get_active_session()

def get_all_tables_in_schema(session, database, schema):
    """Get all tables in a given schema"""
    try:
        query = f"""
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM {database}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{schema}'
        AND TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME
        """
        return session.sql(query).to_pandas()
    except Exception as e:
        st.error(f"Error getting tables from {database}.{schema}: {str(e)}")
        return pd.DataFrame()

def get_all_databases(session):
    """Get all available databases"""
    try:
        query = "SHOW DATABASES"
        databases_df = session.sql(query).to_pandas()
        
        # Check for different possible column names (including quoted versions)
        if '"name"' in databases_df.columns:
            return databases_df['"name"'].tolist()
        elif 'name' in databases_df.columns:
            return databases_df['name'].tolist()
        elif 'NAME' in databases_df.columns:
            return databases_df['NAME'].tolist()
        elif '"NAME"' in databases_df.columns:
            return databases_df['"NAME"'].tolist()
        elif 'database_name' in databases_df.columns:
            return databases_df['database_name'].tolist()
        elif 'DATABASE_NAME' in databases_df.columns:
            return databases_df['DATABASE_NAME'].tolist()
        elif '"database_name"' in databases_df.columns:
            return databases_df['"database_name"'].tolist()
        elif '"DATABASE_NAME"' in databases_df.columns:
            return databases_df['"DATABASE_NAME"'].tolist()
        else:
            # If none of the expected columns exist, show what columns are available
            st.error(f"Unexpected column names in SHOW DATABASES result: {list(databases_df.columns)}")
            # Try to use the first column as database name
            if len(databases_df.columns) > 0:
                return databases_df.iloc[:, 0].tolist()
            return []
    except Exception as e:
        st.error(f"Error getting databases: {str(e)}")
        return []

def get_all_schemas(session, database):
    """Get all schemas in a database"""
    try:
        query = f"SHOW SCHEMAS IN DATABASE {database}"
        schemas_df = session.sql(query).to_pandas()
        
        # Check for different possible column names (including quoted versions)
        if '"name"' in schemas_df.columns:
            return schemas_df['"name"'].tolist()
        elif 'name' in schemas_df.columns:
            return schemas_df['name'].tolist()
        elif 'NAME' in schemas_df.columns:
            return schemas_df['NAME'].tolist()
        elif '"NAME"' in schemas_df.columns:
            return schemas_df['"NAME"'].tolist()
        elif 'schema_name' in schemas_df.columns:
            return schemas_df['schema_name'].tolist()
        elif 'SCHEMA_NAME' in schemas_df.columns:
            return schemas_df['SCHEMA_NAME'].tolist()
        elif '"schema_name"' in schemas_df.columns:
            return schemas_df['"schema_name"'].tolist()
        elif '"SCHEMA_NAME"' in schemas_df.columns:
            return schemas_df['"SCHEMA_NAME"'].tolist()
        else:
            # If none of the expected columns exist, show what columns are available
            st.error(f"Unexpected column names in SHOW SCHEMAS result: {list(schemas_df.columns)}")
            # Try to use the first column as schema name
            if len(schemas_df.columns) > 0:
                return schemas_df.iloc[:, 0].tolist()
            return []
    except Exception as e:
        st.error(f"Error getting schemas from {database}: {str(e)}")
        return []

def compare_table_data_minus(session, db1, schema1, db2, schema2, table_name):
    """
    Compare data between two tables using MINUS operation
    Only perform MINUS if row counts match, else mark as COUNT_MISMATCH
    """
    try:
        table1_full = f"{db1}.{schema1}.{table_name}"
        table2_full = f"{db2}.{schema2}.{table_name}"

        # Get column names for the table and add double quotes
        columns_query = f"""
        SELECT COLUMN_NAME
        FROM {db1}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema1}' AND TABLE_NAME = '{table_name}'
        ORDER BY ORDINAL_POSITION
        """
        columns_df = session.sql(columns_query).to_pandas()
        column_names = columns_df['COLUMN_NAME'].tolist()
        quoted_columns = [f'"{col}"' for col in column_names]
        columns_str = ', '.join(quoted_columns)

        # Get row counts for both tables
        count1_query = f"SELECT COUNT(*) as count FROM {table1_full}"
        count2_query = f"SELECT COUNT(*) as count FROM {table2_full}"

        count1 = session.sql(count1_query).collect()[0]['COUNT']
        count2 = session.sql(count2_query).collect()[0]['COUNT']

        if count1 != count2:
            # If counts don't match, skip MINUS and mark as COUNT_MISMATCH
            return {
                'source_schema': schema1,
                'target_schema': schema2,
                'table_name': table_name,
                'count1': count1,
                'count2': count2,
                'rows_in_table1_not_in_table2': 'N/A',
                'rows_in_table2_not_in_table1': 'N/A',
                'data_match': False,
                'status': 'COUNT_MISMATCH'
            }

        # Perform MINUS operations in both directions using explicit columns
        minus1_query = f"""
        SELECT COUNT(*) as diff_count FROM (
            SELECT {columns_str} FROM {table1_full}
            MINUS
            SELECT {columns_str} FROM {table2_full}
        )
        """

        minus2_query = f"""
        SELECT COUNT(*) as diff_count FROM (
            SELECT {columns_str} FROM {table2_full}
            MINUS
            SELECT {columns_str} FROM {table1_full}
        )
        """

        diff1 = session.sql(minus1_query).collect()[0]['DIFF_COUNT']
        diff2 = session.sql(minus2_query).collect()[0]['DIFF_COUNT']

        # Determine if tables match
        tables_match = (diff1 == 0 and diff2 == 0 and count1 == count2)

        return {
            'source_schema': schema1,
            'target_schema': schema2,
            'table_name': table_name,
            'count1': count1,
            'count2': count2,
            'rows_in_table1_not_in_table2': diff1,
            'rows_in_table2_not_in_table1': diff2,
            'data_match': tables_match,
            'status': 'MATCH' if tables_match else 'MISMATCH'
        }

    except Exception as e:
        return {
            'source_schema': schema1,
            'target_schema': schema2,
            'table_name': table_name,
            'count1': 'ERROR',
            'count2': 'ERROR',
            'rows_in_table1_not_in_table2': 'ERROR',
            'rows_in_table2_not_in_table1': 'ERROR',
            'data_match': False,
            'status': 'ERROR',
            'error': str(e)
        }

def run_selected_tables_comparison(session, db1, schema1, db2, schema2, selected_tables):
    """
    Compare specific selected tables between two schemas
    """
    all_results = []
    
    if not selected_tables:
        st.warning("No tables selected for comparison")
        return pd.DataFrame()
    
    # Create progress tracking
    total_tables = len(selected_tables)
    current_table = 0
    
    # Create progress containers
    progress_container = st.container()
    with progress_container:
        main_progress = st.progress(0)
        status_text = st.empty()
    
    # Compare each selected table
    for table_name in selected_tables:
        current_table += 1
        main_progress.progress(current_table / total_tables)
        status_text.text(f'Comparing table: {table_name} ({current_table}/{total_tables})')
        
        # Check if table exists in target schema
        try:
            count2_query = f"SELECT COUNT(*) as count FROM {db2}.{schema2}.{table_name}"
            a = session.sql(count2_query).collect()[0]['COUNT']
            table_exists_in_schema2 = True
        except:
            table_exists_in_schema2 = False
        
        if table_exists_in_schema2:
            # Both tables exist, compare data
            result = compare_table_data_minus(session, db1, schema1, db2, schema2, table_name)
        else:
            # Table only exists in source schema
            try:
                count1 = session.sql(f"SELECT COUNT(*) as count FROM {db1}.{schema1}.{table_name}").collect()[0]['COUNT']
            except:
                count1 = 'ERROR'
            result = {
                'source_schema': schema1,
                'target_schema': schema2,
                'table_name': table_name,
                'count1': count1,
                'count2': 'N/A',
                'rows_in_table1_not_in_table2': 'N/A',
                'rows_in_table2_not_in_table1': 'N/A',
                'data_match': False,
                'status': 'ONLY_IN_SOURCE'
            }
        
        all_results.append(result)
    
    # Clear progress tracking completely
    progress_container.empty()
    
    return pd.DataFrame(all_results)
def run_multiple_schema_comparison(session, db1, schemas1_list, db2, schemas2_list):
    """
    Compare tables across multiple schemas (one-to-one mapping)
    """
    all_results = []
    
    # Ensure both lists have the same length for one-to-one comparison
    if len(schemas1_list) != len(schemas2_list):
        st.error(f"Number of source schemas ({len(schemas1_list)}) must match number of target schemas ({len(schemas2_list)}) for one-to-one comparison")
        return pd.DataFrame()
    
    # Create progress tracking
    total_comparisons = len(schemas1_list)
    current_comparison = 0
    
    # Create progress containers
    progress_container = st.container()
    with progress_container:
        main_progress = st.progress(0)
        status_text = st.empty()
    
    # Compare schemas one-to-one
    for i, (schema1, schema2) in enumerate(zip(schemas1_list, schemas2_list)):
        schema1 = schema1.strip()
        schema2 = schema2.strip()
        current_comparison += 1
        
        main_progress.progress(current_comparison / total_comparisons)
        #status_text.text(f'Comparing {schema1} with {schema2}')
        
        # Get tables from source schema
        tables1 = get_all_tables_in_schema(session, db1, schema1)
        
        if tables1.empty:
            st.warning(f"No tables found in {db1}.{schema1}")
            continue
            
        table_names = tables1['TABLE_NAME'].tolist()
        schema_results = []
        
        # Process tables without showing individual table progress
        for table_name in sorted(table_names):
            
            # Check if table exists in target schema
            try:
                count2_query = f"SELECT COUNT(*) as count FROM {db2}.{schema2}.{table_name}"
                a = session.sql(count2_query).collect()[0]['COUNT']
                table_exists_in_schema2 = True
            except:
                table_exists_in_schema2 = False
            
            if table_exists_in_schema2:
                # Both tables exist, compare data
                result = compare_table_data_minus(session, db1, schema1, db2, schema2, table_name)
            else:
                # Table only exists in source schema
                try:
                    count1 = session.sql(f"SELECT COUNT(*) as count FROM {db1}.{schema1}.{table_name}").collect()[0]['COUNT']
                except:
                    count1 = 'ERROR'
                result = {
                    'source_schema': schema1,
                    'target_schema': schema2,
                    'table_name': table_name,
                    'count1': count1,
                    'count2': 'N/A',
                    'rows_in_table1_not_in_table2': 'N/A',
                    'rows_in_table2_not_in_table1': 'N/A',
                    'data_match': False,
                    'status': 'ONLY_IN_SOURCE'
                }
            
            schema_results.append(result)
        
        all_results.extend(schema_results)
    
    # Clear progress tracking completely
    progress_container.empty()
    
    return pd.DataFrame(all_results)
# Streamlit UI
st.set_page_config(page_title="Schema Data Comparison Tool", layout="wide", initial_sidebar_state="expanded")

# Custom CSS for black and white theme
st.markdown("""
<style>
    .main > div {
        padding-left: 1rem;
        padding-right: 1rem;
        font-size: 0.85rem;
        padding-top: 10rem;
    }
    .block-container {
        padding-top: 1rem;
        padding-bottom: 0rem;
        margin-top: 0rem;
    }
    h1 {
        text-align: center !important;
        margin-top: 0rem !important;
        margin-bottom: 0.5rem !important;
        padding-top: 0rem !important;
        position: fixed !important;
        top: 0 !important;
        left: 0 !important;
        right: 0 !important;
        background-color: white !important;
        z-index: 999 !important;
        padding: 1rem !important;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1) !important;
    }
    .stMarkdown p {
        text-align: center !important;
        margin-bottom: 1rem !important;
        position: fixed !important;
        top: 3.5rem !important;
        left: 0 !important;
        right: 0 !important;
        background-color: white !important;
        z-index: 998 !important;
        padding: 0.5rem 1rem !important;
        box-shadow: 0 1px 2px rgba(0,0,0,0.05) !important;
    }
    .input-section {
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 6px;
        border: 1px solid #dee2e6;
        margin-bottom: 1rem;
        min-height: 550px;
    }
    .output-section {
        background-color: #ffffff;
        padding: 0.8rem;
        border-radius: 6px;
        border: 1px solid #dee2e6;
        min-height: 400px;
    }
    .nav-header {
        background: linear-gradient(90deg, #343a40 0%, #495057 100%);
        color: white;
        padding: 0.8rem;
        border-radius: 6px;
        margin-bottom: 1rem;
        text-align: center;
        font-weight: bold;
        font-size: 0.9rem;
    }
    .section-divider {
        margin: 1rem 0;
        border-top: 1px solid #dee2e6;
    }
    .stSelectbox label, .stMultiSelect label {
        font-size: 0.8rem !important;
        margin-bottom: 0.2rem !important;
        color: #212529 !important;
    }
    .stMarkdown h3 {
        font-size: 1rem !important;
        margin-top: 0.5rem !important;
        margin-bottom: 0.3rem !important;
        color: #212529 !important;
    }
    .stInfo, .stWarning, .stError {
        font-size: 0.8rem !important;
        padding: 0.5rem !important;
    }
    .stButton button {
        font-size: 0.85rem !important;
        padding: 0.4rem 1rem !important;
        background-color: #212529 !important;
        color: white !important;
        border: 1px solid #212529 !important;
    }
    .stButton button:hover {
        background-color: #495057 !important;
        border-color: #495057 !important;
    }
    .stMetric {
        font-size: 0.8rem !important;
    }
    .stDataFrame {
        font-size: 0.75rem !important;
    }
    .stTabs [data-baseweb="tab-list"] {
        background-color: #f8f9fa !important;
        border-bottom: 2px solid #dee2e6 !important;
        border-radius: 8px 8px 0 0 !important;
        padding: 0.25rem !important;
        margin-bottom: 1rem !important;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1) !important;
        position: relative !important;
        z-index: 997 !important;
        margin-top: 0.5rem !important;
        width: 100% !important;
        display: flex !important;
        justify-content: flex-start !important;
    }
    .stTabs [data-baseweb="tab"] {
        background-color: white !important;
        color: #495057 !important;
        border: 1px solid #dee2e6 !important;
        border-radius: 6px !important;
        margin: 0.25rem !important;
        padding: 0.6rem 1.2rem !important;
        font-weight: 500 !important;
        font-size: 0.9rem !important;
        transition: all 0.2s ease !important;
        box-shadow: 0 1px 2px rgba(0,0,0,0.05) !important;
        min-width: 200px !important;
        text-align: center !important;
    }
    .stTabs [data-baseweb="tab"]:hover {
        background-color: #f8f9fa !important;
        border-color: #adb5bd !important;
        transform: translateY(-1px) !important;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1) !important;
    }
    .stTabs [aria-selected="true"] {
        background-color: #212529 !important;
        color: white !important;
        border-color: #212529 !important;
        font-weight: 600 !important;
        transform: translateY(-2px) !important;
        box-shadow: 0 4px 8px rgba(0,0,0,0.15) !important;
    }
    .stTabs [data-baseweb="tab-panel"] {
        padding-top: 1rem !important;
        border-radius: 0 0 8px 8px !important;
        background-color: white !important;
    }
    .stSelectbox > div > div {
        background-color: white !important;
        border: 1px solid #ced4da !important;
        min-height: 38px !important;
    }
    .stMultiSelect > div > div {
        background-color: white !important;
        border: 1px solid #ced4da !important;
        min-height: 38px !important;
        padding: 0.25rem !important;
    }
    .stMultiSelect [data-baseweb="select"] {
        min-height: 38px !important;
        padding-bottom: 0rem !important;
    }
    .stSelectbox [data-baseweb="select"] {
        min-height: 38px !important;
        margin-bottom: 0rem !important;
    }
    
    /* Fix multiselect dropdown positioning and visibility */
    .stMultiSelect > div > div > div[data-baseweb="popover"] {
        z-index: 1000 !important;
    }
    
    /* Ensure proper spacing for form elements */
    .stSelectbox, .stMultiSelect {
        margin-bottom: 0.5rem !important;
        padding-bottom: 0rem !important;
    }
    
    /* Remove extra spacing from form containers */
    .stSelectbox > div, .stMultiSelect > div {
        margin-bottom: 0rem !important;
        padding-bottom: 0rem !important;
    }
    
    /* Enhanced download button styling for black theme */
    .download-section {
        background: linear-gradient(135deg, #212529 0%, #343a40 100%);
        padding: 1rem;
        border-radius: 8px;
        margin: 1rem 0;
        border: 1px solid #495057;
        box-shadow: 0 4px 8px rgba(0,0,0,0.1);
        display: none !important;
    }
    
    .download-header {
        color: white;
        font-size: 1rem;
        font-weight: 600;
        margin-bottom: 0.5rem;
        text-align: center;
    }
    
    /* Download button styling to match primary buttons */
    .stDownloadButton {
        display: none !important;
    }
    
    .stDownloadButton button {
        display: none !important;
    }
    
    
    /* Metrics styling enhancement */
    .metric-container {
        background-color: #f8f9fa;
        padding: 0.5rem;
        border-radius: 6px;
        border: 1px solid #dee2e6;
        text-align: center;
    }
</style>
""", unsafe_allow_html=True)

st.title("🔍 Snowflake Data Comparison Tool")
st.markdown("<p style='text-align: center; color: #666; margin-bottom: 2rem;'>Compare tables between schemas with flexible selection options</p>", unsafe_allow_html=True)

# Create tabs for different comparison modes
st.markdown("")
st.markdown("")

tab1, tab2 = st.tabs(["🎯 Selected Tables Comparison", "📊 Multiple Schema Comparison"])

with tab1:
    st.subheader("Compare Selected Tables Between Two Schemas")
    
    # Create main layout with 30-70 ratio
    col_input, col_output = st.columns([3, 7], gap="medium")
    
    with col_input:
        # Input container with enhanced styling
        with st.container():
            st.markdown('<div class="nav-header">📝 INPUT CONFIGURATION </div>', unsafe_allow_html=True)
            
            st.info("📌 Select databases, schemas, and tables to compare")
            
            # Get available databases
            available_databases = get_all_databases(session)
            
            # Source Database & Schema section
            col_db1, col_schema1 = st.columns(2)
            with col_db1:
                    db1 = st.selectbox("Source Database:", available_databases, key="source_db")
            with col_schema1:
                    available_schemas1 = get_all_schemas(session, db1) if db1 else []
                    schema1 = st.selectbox("Source Schema:", available_schemas1, key="source_schema")
            if db1 and schema1:
                    tables_df1 = get_all_tables_in_schema(session, db1, schema1)
                    if not tables_df1.empty:
                        available_tables = tables_df1['TABLE_NAME'].tolist()
                        selected_tables = st.multiselect(
                            "Source Tables:", 
                            available_tables,
                            help="Select one or more tables to compare",
                            key="selected_tables"
                        )
                    else:
                        st.warning(f"⚠️ No tables found in {db1}.{schema1}")
                        selected_tables = []
            
            st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
            
            # Target Database & Schema section
            col_db2, col_schema2 = st.columns(2)
            with col_db2:
                db2 = st.selectbox("Target Database:", available_databases, key="target_db")
            with col_schema2:
                available_schemas2 = get_all_schemas(session, db2) if db2 else []
                schema2 = st.selectbox("Target Schema:", available_schemas2, key="target_schema")
            if db2 and schema2:
                tables_df2 = get_all_tables_in_schema(session, db2, schema2)
                if not tables_df2.empty:
                    available_tables2 = tables_df2['TABLE_NAME'].tolist()
                    selected_tables2 = st.multiselect(
                        "Target Tables:", 
                        available_tables2,
                        help="Tables available in the target schema for reference",
                        key="available_tables2"
                    )
                else:
                    st.warning(f"⚠️ No tables found in {db2}.{schema2}")
                    selected_tables2 = []
            
            st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
            
            # Comparison button
            compare_clicked = st.button("🚀 Compare Selected Tables", type="primary", key="compare_selected", use_container_width=True)
            
            st.markdown('</div></div>', unsafe_allow_html=True)
    
    with col_output:
        # Output section styling
        st.markdown('<div class="nav-header">📊 COMPARISON RESULTS</div>', unsafe_allow_html=True)
        
        with st.container():
            
            # Show comparison results
            if compare_clicked:
                if db1 and schema1 and db2 and schema2 and selected_tables:
                    st.markdown(f"""
                    **📋 Comparison Summary:**
                    - **Tables to Compare:** {len(selected_tables)} table(s)
                    - **Source:** `{db1}.{schema1}`
                    - **Target:** `{db2}.{schema2}`
                    - **Table Name:** `{selected_tables}`
                    """)
                    
                    with st.spinner("🔄 Comparing selected tables..."):
                        comparison_results = run_selected_tables_comparison(
                            session, db1, schema1, db2, schema2, selected_tables
                        )
                        
                        if not comparison_results.empty:
                            # Color code the status column
                            def color_status(val):
                                if val == 'MATCH':
                                    return 'background-color: #d4edda; color: #155724;'
                                elif val == 'MISMATCH':
                                    return 'background-color: #f8d7da; color: #721c24;'
                                elif val == 'ONLY_IN_SOURCE':
                                    return 'background-color: #fff3cd; color: #856404;'
                                elif val == 'COUNT_MISMATCH':
                                    return 'background-color: #ffeaa7; color: #6c5ce7;'
                                elif val == 'ERROR':
                                    return 'background-color: #f8d7da; color: #721c24;'
                                return ''
                            
                            styled_df = comparison_results.style.map(color_status, subset=['status'])
                            st.dataframe(styled_df, use_container_width=True, height=290, hide_index=True)
                            
                            # Summary statistics with enhanced styling
                            status_counts = comparison_results['status'].value_counts()
                            st.markdown("### 📈 Summary Statistics")
                            
                            col1, col2, col3 = st.columns(3)
                            with col1:
                                st.markdown("""
                                <div class="metric-container">
                                    <div style="font-size: 0.8rem; color: #666;">📊 Total Tables</div>
                                    <div style="font-size: 1 rem; font-weight: bold; color: #212529;">{}</div>
                                </div>
                                """.format(len(comparison_results)), unsafe_allow_html=True)
                            with col2:
                                st.markdown("""
                                <div class="metric-container">
                                    <div style="font-size: 0.8rem; color: #666;">✅ Matches</div>
                                    <div style="font-size: 1 rem; font-weight: bold; color: #28a745;">{}</div>
                                </div>
                                """.format(status_counts.get('MATCH', 0)), unsafe_allow_html=True)
                            with col3:
                                st.markdown("""
                                <div class="metric-container">
                                    <div style="font-size: 0.8rem; color: #666;">❌ Mismatches</div>
                                    <div style="font-size: 1 rem; font-weight: bold; color: #dc3545;">{}</div>
                                </div>
                                """.format(status_counts.get('MISMATCH', 0) + status_counts.get('COUNT_MISMATCH', 0)), unsafe_allow_html=True)
                            
                            # Show tables that only exist in source
                            only_in_source_df = comparison_results[comparison_results['status'] == 'ONLY_IN_SOURCE']
                            if not only_in_source_df.empty:
                                st.markdown("### ⚠️ Tables Only in Source Schema")
                                st.dataframe(only_in_source_df, use_container_width=True, hide_index=True)
                        else:
                            st.error("❌ No comparison results generated")
                else:
                    st.error("⚠️ Please select databases, schemas, and at least one table to compare")
            else:
                st.info("👈 **Get Started:** Configure your comparison settings and click 'Compare Selected Tables' to see results here.")
            
            st.markdown('</div>', unsafe_allow_html=True)

with tab2:
    st.subheader("Multiple Schema Comparison (One-to-One)")
    
    # Create main layout with 30-70 ratio
    col_input2, col_output2 = st.columns([3, 7], gap="medium")
    
    with col_input2:
        # Input container with enhanced styling
        with st.container():
            st.markdown('<div class="nav-header">📝 INPUT CONFIGURATION </div>', unsafe_allow_html=True)

            
            st.info("📌 Note: Schemas will be compared one-to-one in order. Number of source and target schemas must match.")
            
            # Get available databases
            available_databases = get_all_databases(session)
            
            # Source Schemas section
            st.markdown("### 🔹 Source Schemas (Database1)")
            col_db_multi1, col_schema_multi1 = st.columns(2)
            with col_db_multi1:
                db1_multi = st.selectbox("Database 1:", available_databases, key="db1_multi")
            with col_schema_multi1:
                available_schemas1_multi = get_all_schemas(session, db1_multi) if db1_multi else []
                selected_schemas1 = st.multiselect(
                    "Source Schemas:", 
                    available_schemas1_multi,
                    help="Select multiple schemas to compare",
                    key="selected_schemas1"
                )
            
            st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
            
            # Target Schemas section
            st.markdown("### 🔹 Target Schemas (Database2)")
            col_db_multi2, col_schema_multi2 = st.columns(2)
            with col_db_multi2:
                db2_multi = st.selectbox("Database 2:", available_databases, key="db2_multi")
            with col_schema_multi2:
                available_schemas2_multi = get_all_schemas(session, db2_multi) if db2_multi else []
                selected_schemas2 = st.multiselect(
                    "Target Schemas:", 
                    available_schemas2_multi,
                    help="Select schemas in same order as source schemas",
                    key="selected_schemas2"
                )
            
            # Show the mapping
            if 'selected_schemas1' in locals() and 'selected_schemas2' in locals():
                if selected_schemas1 and selected_schemas2:
                    if len(selected_schemas1) != len(selected_schemas2):
                        st.warning(f"⚠️ Number of source schemas ({len(selected_schemas1)}) must match number of target schemas ({len(selected_schemas2)})")
            
            st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
            
            # Comparison button
            compare_multiple_clicked = st.button("🚀 Compare Multiple Schemas", type="primary", key="compare_multiple", use_container_width=True)
            
            st.markdown('</div></div>', unsafe_allow_html=True)
    
    with col_output2:
        # Output section styling
        st.markdown('<div class="nav-header">📊 COMPARISON RESULTS</div>', unsafe_allow_html=True)
        
        with st.container():
            
            # Show comparison results
            if compare_multiple_clicked:
                if ('selected_schemas1' in locals() and 'selected_schemas2' in locals() and 
                    db1_multi and selected_schemas1 and db2_multi and selected_schemas2):
                    
                    if len(selected_schemas1) != len(selected_schemas2):
                        st.error(f"⚠️ Number of source schemas ({len(selected_schemas1)}) must match number of target schemas ({len(selected_schemas2)})")
                    else:
                        st.markdown(f"""
                        **📋 Comparison Summary:**
                        - **Schema Pairs:** {len(selected_schemas1)} pair(s) one-to-one
                        """)
                        
                        for i, (s1, s2) in enumerate(zip(selected_schemas1, selected_schemas2), 1):
                            st.write(f"**Pair {i}:** `{s1}` ↔ `{s2}`")
                        
                        with st.spinner():
                            comparison_results = run_multiple_schema_comparison(
                                session, db1_multi, selected_schemas1, db2_multi, selected_schemas2
                            )
                            
                            if not comparison_results.empty:
                                # Color code the status column
                                def color_status(val):
                                    if val == 'MATCH':
                                        return 'background-color: #d4edda; color: #155724;'
                                    elif val == 'MISMATCH':
                                        return 'background-color: #f8d7da; color: #721c24;'
                                    elif val == 'ONLY_IN_SOURCE':
                                        return 'background-color: #fff3cd; color: #856404;'
                                    elif val == 'COUNT_MISMATCH':
                                        return 'background-color: #ffeaa7; color: #6c5ce7;'
                                    elif val == 'ERROR':
                                        return 'background-color: #f8d7da; color: #721c24;'
                                    return ''
                                
                                styled_df = comparison_results.style.map(color_status, subset=['status'])
                                st.dataframe(styled_df, use_container_width=True, height=290, hide_index=True)
                                
                                # Summary statistics with enhanced styling
                                status_counts = comparison_results['status'].value_counts()
                                st.markdown("### 📈 Summary Statistics")
                                
                                col1, col2, col3, col4 = st.columns(4)
                                with col1:
                                    st.markdown("""
                                    <div class="metric-container">
                                        <div style="font-size: 0.8rem; color: #666;">📊 Total Tables</div>
                                        <div style="font-size: 1.5rem; font-weight: bold; color: #212529;">{}</div>
                                    </div>
                                    """.format(len(comparison_results)), unsafe_allow_html=True)
                                with col2:
                                    st.markdown("""
                                    <div class="metric-container">
                                        <div style="font-size: 0.8rem; color: #666;">✅ Matches</div>
                                        <div style="font-size: 1.5rem; font-weight: bold; color: #28a745;">{}</div>
                                    </div>
                                    """.format(status_counts.get('MATCH', 0)), unsafe_allow_html=True)
                                with col3:
                                    st.markdown("""
                                    <div class="metric-container">
                                        <div style="font-size: 0.8rem; color: #666;">❌ Mismatches</div>
                                        <div style="font-size: 1.5rem; font-weight: bold; color: #dc3545;">{}</div>
                                    </div>
                                    """.format(status_counts.get('MISMATCH', 0) + status_counts.get('COUNT_MISMATCH', 0)), unsafe_allow_html=True)
                                with col4:
                                    st.markdown("""
                                    <div class="metric-container">
                                        <div style="font-size: 0.8rem; color: #666;">⚠️ Only in Source</div>
                                        <div style="font-size: 1.5rem; font-weight: bold; color: #ffc107;">{}</div>
                                    </div>
                                    """.format(status_counts.get('ONLY_IN_SOURCE', 0)), unsafe_allow_html=True)
                                
                                # Show tables that only exist in source
                                only_in_source_df = comparison_results[comparison_results['status'] == 'ONLY_IN_SOURCE']
                                if not only_in_source_df.empty:
                                    st.markdown("### ⚠️ Tables Only in Source Schema(s)")
                                    st.dataframe(only_in_source_df, use_container_width=True, hide_index=True)
                            else:
                                st.error("❌ No comparison results generated")
                else:
                    st.error("⚠️ Please select databases and schemas for comparison")
            else:
                st.info("👈 **Get Started:** Configure your comparison settings and click 'Compare Multiple Schemas' to see results here.")
            
            st.markdown('</div>', unsafe_allow_html=True)

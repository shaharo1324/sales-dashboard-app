import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
from databricks import sql
from databricks.sdk.core import Config
from typing import Any, Dict, List, Optional
import os
import plotly.graph_objects as go
import math 

# Testing Automatic Deployment

st.set_page_config(layout="wide")

# Custom CSS for the Apply Filters button (Soft Red)
st.markdown("""
<style>
[data-testid="stForm"] button {
    border-color: #ffcccc !important;
    background-color: #3f51b5 !important;
    color: #ffcccc !important;
    background-image: none !important;
    box-shadow: none !important;
}

/* Hover state */
[data-testid="stForm"] button:hover {
    border-color: #cc0000 !important;
    background-color: #2d2a8a !important; /* Slightly darker for hover */
    color: #cc0000 !important;
    background-image: none !important;
    box-shadow: none !important;
}

/* Focus/Active state (when clicked) */
[data-testid="stForm"] button:focus, [data-testid="stForm"] button:active {
    background-color: #3f3bb8 !important;
    color: #cc0000 !important;
    border-color: #ffcccc !important;
    box-shadow: none !important;
}
</style>
""", unsafe_allow_html=True)

st.header("Sales Dashboard")

# Invisible anchor at top for scroll-to-top functionality
st.markdown('<div id="top-anchor"></div>', unsafe_allow_html=True)

# Help popover with usage instructions and contact info
with st.popover("📖 Info"):
    st.markdown("""
    ### Sales Dashboard
    
    Explore general and cross-organization statistics about our data globally.
    
    **🔘 Applying Filters:** Filters are not applied automatically. After selecting 
    your desired filters in the sidebar, click the **"Apply Filters & Refresh"** 
    button to update the dashboard.
    
    **💡 Tip:** When filtering by a specific **Vendor** or **Model**, you'll have 
    an option at the bottom of the Global tab to retrieve UID examples with the 
    best classification combinations.
    
    ---
    
    **Need assistance?**  
    Feel free to contact me for any purpose — whether it's requesting new features 
    for the dashboard, reporting an error, or flagging unexpected data discrepancies.
    
    📧 [shahar.o@claroty.com](mailto:shahar.o@claroty.com)  
    💬 [Slack](https://claroty.enterprise.slack.com/team/U06L8BEFXPT)
    """)

MAIN_SQL_HTTP_PATH = "/sql/1.0/warehouses/472969065f3aed02"

cfg = Config()

@st.cache_resource
def get_connection() -> Any:
    """Create and cache a Databricks SQL connection."""
    return sql.connect(
        server_hostname=cfg.host,
        http_path=MAIN_SQL_HTTP_PATH,
        credentials_provider=lambda: cfg.authenticate,
    )

def execute_sql_query(query: str) -> List[Any]:
    """Executes a SQL query with automatic retry on connection failure."""
    max_retries = 1
    
    for attempt in range(max_retries + 1):
        cursor = None
        try:
            connection = get_connection()
            cursor = connection.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            return result
        except Exception as e:
            if attempt < max_retries:
                print(f"Database error (Attempt {attempt+1}/{max_retries+1}): {str(e)}. Retrying...")
                get_connection.clear()
            else:
                raise e
        finally:
            if cursor:
                try:
                    cursor.close()
                except:
                    pass

@st.cache_data(ttl=3600)  # Cache for 1 hour
def get_filter_options() -> Optional[Dict[str, List[str]]]:
    """Fetch unique values for all filter fields using pre-calculated tables."""
    try:
        # Query 1: Organization filters
        query_org = """
        SELECT
            region,
            vertical,
            organization,
            industry,
            account_status
        FROM `s3-write-bucket`.sales_dashboard.organization_filters
        """
        results_org = execute_sql_query(query_org)
        
        # Query 2: Device filters
        query_device = """
        SELECT
            vendor,
            device_type_family,
            device_subcategory,
            device_category,
            model,
            os_name,
            mac_oui
        FROM `s3-write-bucket`.sales_dashboard.device_filters
        """
        results_device = execute_sql_query(query_device)
        
        # Extract unique values for each field
        filter_options = {
            # Organization filters
            'region': sorted(set(r[0] for r in results_org if r[0])),
            'vertical': sorted(set(r[1] for r in results_org if r[1])),
            'organization': sorted(set(r[2] for r in results_org if r[2])),
            'industry': sorted(set(r[3] for r in results_org if r[3])),
            'account_status': sorted(set(r[4] for r in results_org if r[4])),
            
            # Device filters
            'vendor': sorted(set(r[0] for r in results_device if r[0])),
            'device_type_family': sorted(set(r[1] for r in results_device if r[1])),
            'device_subcategory': sorted(set(r[2] for r in results_device if r[2])),
            'device_category': sorted(set(r[3] for r in results_device if r[3])),
            'model': sorted(set(r[4] for r in results_device if r[4])),
            'os_name': sorted(set(r[5] for r in results_device if r[5])),
            'mac_oui': sorted(set(r[6] for r in results_device if r[6]))
        }
        
        return filter_options
    except Exception as e:
        # Don't show st.error here to avoid it getting stuck in UI
        print(f"Error fetching filter options: {str(e)}")
        return None

@st.cache_data
def get_global_stats(
    region: Optional[str] = None,
    vertical: Optional[str] = None,
    organization: Optional[str] = None,
    industry: Optional[str] = None,
    account_status: Optional[str] = None,
    vendor: Optional[str] = None,
    device_category: Optional[str] = None,
    device_type_family: Optional[str] = None,
    device_subcategory: Optional[str] = None,
    model: Optional[str] = None,
    os_name: Optional[str] = None,
    mac_oui: Optional[str] = None
) -> Dict[str, pd.DataFrame]:
    """Fetch aggregated statistics for the Global tab."""
    try:
        # Build WHERE clause
        where_clause = build_where_clause(
            region, vertical, organization, industry, account_status, vendor, device_category,
            device_type_family, device_subcategory, model, os_name, mac_oui
        )
        
        # Query 1: Top Devices (Aggregated) - Without null filtering
        # We'll filter nulls in Python to reuse data for multiple tables
        query_top_devices = f"""
        SELECT 
            vendor,
            device_type_family,
            model,
            COUNT(*) as count
        FROM `s3-write-bucket`.sales_dashboard.displayable_devices
        WHERE {where_clause}
        GROUP BY vendor, device_type_family, model
        ORDER BY count DESC
        LIMIT 5000
        """
        
        # Query 2: Device Subcategory Distribution (Aggregated)
        query_subcategory = f"""
        SELECT 
            device_subcategory,
            COUNT(*) as count
        FROM `s3-write-bucket`.sales_dashboard.displayable_devices
        WHERE {where_clause}
            AND device_subcategory IS NOT NULL
        GROUP BY device_subcategory
        ORDER BY count DESC
        """
        
        # Query 3: Device Category Distribution (Aggregated)
        query_category = f"""
        SELECT 
            device_category,
            COUNT(*) as count
        FROM `s3-write-bucket`.sales_dashboard.displayable_devices
        WHERE {where_clause}
            AND device_category IS NOT NULL
        GROUP BY device_category
        ORDER BY count DESC
        """
        
        # Query 4: OS Name Distribution (Aggregated)
        query_os = f"""
        SELECT 
            os_name,
            COUNT(*) as count
        FROM `s3-write-bucket`.sales_dashboard.displayable_devices
        WHERE {where_clause}
            AND os_name IS NOT NULL
        GROUP BY os_name
        ORDER BY count DESC
        """

        # Query 5: Top 20 Vendors (Aggregated)
        query_vendor = f"""
        SELECT 
            vendor,
            COUNT(*) as count
        FROM `s3-write-bucket`.sales_dashboard.displayable_devices
        WHERE {where_clause}
            AND vendor IS NOT NULL
        GROUP BY vendor
        ORDER BY count DESC
        LIMIT 20
        """
        
        # Query 6: Total Device Count
        query_total = f"""
        SELECT COUNT(*) as total_count
        FROM `s3-write-bucket`.sales_dashboard.displayable_devices
        WHERE {where_clause}
        """
        
        # Query 7: Source Coverage (devices per source from all_seen_sources array)
        query_sources = f"""
        SELECT 
            source,
            COUNT(*) as device_count
        FROM (
            SELECT DISTINCT organization, uid, exploded_source as source
            FROM `s3-write-bucket`.sales_dashboard.displayable_devices
            LATERAL VIEW explode(all_seen_sources) AS exploded_source
            WHERE {where_clause}
        )
        GROUP BY source
        ORDER BY device_count DESC
        """
        
        # Query 8: Organization Distribution (all organizations, for CSV download)
        query_org_dist = f"""
        SELECT 
            organization,
            COUNT(*) as count
        FROM `s3-write-bucket`.sales_dashboard.displayable_devices
        WHERE {where_clause}
            AND organization IS NOT NULL
        GROUP BY organization
        ORDER BY count DESC
        """
        
        results_top = execute_sql_query(query_top_devices)
        df_top = pd.DataFrame(results_top, columns=['vendor', 'device_type_family', 'model', 'count'])
        
        results_sub = execute_sql_query(query_subcategory)
        df_sub = pd.DataFrame(results_sub, columns=['device_subcategory', 'count'])
        
        results_cat = execute_sql_query(query_category)
        df_cat = pd.DataFrame(results_cat, columns=['device_category', 'count'])
        
        results_os = execute_sql_query(query_os)
        df_os = pd.DataFrame(results_os, columns=['os_name', 'count'])

        results_vendor = execute_sql_query(query_vendor)
        df_vendor = pd.DataFrame(results_vendor, columns=['vendor', 'count'])
        
        results_total = execute_sql_query(query_total)
        total_devices = results_total[0][0] if results_total else 0
        
        results_sources = execute_sql_query(query_sources)
        df_sources = pd.DataFrame(results_sources, columns=['source', 'device_count'])
        
        results_org_dist = execute_sql_query(query_org_dist)
        df_org_dist = pd.DataFrame(results_org_dist, columns=['organization', 'count'])
        
        return {
            "top_devices": df_top,
            "subcategory": df_sub,
            "category": df_cat,
            "os_dist": df_os,
            "vendor_dist": df_vendor,
            "total_devices": total_devices,
            "source_coverage": df_sources,
            "org_dist": df_org_dist
        }
        
    except Exception as e:
        st.error(f"Error querying Global stats: {str(e)}")
        return {
            "top_devices": pd.DataFrame(),
            "subcategory": pd.DataFrame(),
            "category": pd.DataFrame(),
            "os_dist": pd.DataFrame(),
            "vendor_dist": pd.DataFrame(),
            "total_devices": 0,
            "source_coverage": pd.DataFrame(),
            "org_dist": pd.DataFrame()
        }

@st.cache_data
def get_risk_stats(
    region: Optional[str] = None,
    vertical: Optional[str] = None,
    organization: Optional[str] = None,
    industry: Optional[str] = None,
    account_status: Optional[str] = None,
    vendor: Optional[str] = None,
    device_category: Optional[str] = None,
    device_type_family: Optional[str] = None,
    device_subcategory: Optional[str] = None,
    model: Optional[str] = None,
    os_name: Optional[str] = None,
    mac_oui: Optional[str] = None
) -> Dict[str, pd.DataFrame]:
    """Fetch aggregated statistics for the Risk tab."""
    try:
        # Build WHERE clause
        where_clause = build_where_clause(
            region, vertical, organization, industry, account_status, vendor, device_category,
            device_type_family, device_subcategory, model, os_name, mac_oui
        )
        
        # Query 4: Risk Score Distribution
        query_risk_dist = f"""
        SELECT 
            risk_score,
            COUNT(*) as count
        FROM `s3-write-bucket`.sales_dashboard.displayable_devices
        WHERE {where_clause}
            AND risk_score IS NOT NULL
        GROUP BY risk_score
        ORDER BY count DESC
        """
        
        # Query 5: Top Devices by Risk Score (Critical)
        query_risk_critical = f"""
        SELECT 
            vendor,
            device_type_family,
            model,
            COUNT(*) as count
        FROM `s3-write-bucket`.sales_dashboard.displayable_devices
        WHERE {where_clause}
            AND risk_score = 'Critical'
            AND vendor IS NOT NULL 
            AND device_type_family IS NOT NULL 
            AND model IS NOT NULL
        GROUP BY vendor, device_type_family, model
        ORDER BY count DESC
        LIMIT 100
        """
        
        # Query 6: Top Devices by Risk Score (High)
        query_risk_high = f"""
        SELECT 
            vendor,
            device_type_family,
            model,
            COUNT(*) as count
        FROM `s3-write-bucket`.sales_dashboard.displayable_devices
        WHERE {where_clause}
            AND risk_score = 'High'
            AND vendor IS NOT NULL 
            AND device_type_family IS NOT NULL 
            AND model IS NOT NULL
        GROUP BY vendor, device_type_family, model
        ORDER BY count DESC
        LIMIT 100
        """
        
        # Query 7: Top Devices by Risk Score (Medium)
        query_risk_medium = f"""
        SELECT 
            vendor,
            device_type_family,
            model,
            COUNT(*) as count
        FROM `s3-write-bucket`.sales_dashboard.displayable_devices
        WHERE {where_clause}
            AND risk_score = 'Medium'
            AND vendor IS NOT NULL 
            AND device_type_family IS NOT NULL 
            AND model IS NOT NULL
        GROUP BY vendor, device_type_family, model
        ORDER BY count DESC
        LIMIT 100
        """
        
        results_risk = execute_sql_query(query_risk_dist)
        df_risk = pd.DataFrame(results_risk, columns=['risk_score', 'count'])
        
        results_critical = execute_sql_query(query_risk_critical)
        df_critical = pd.DataFrame(results_critical, columns=['vendor', 'device_type_family', 'model', 'count'])
        
        results_high = execute_sql_query(query_risk_high)
        df_high = pd.DataFrame(results_high, columns=['vendor', 'device_type_family', 'model', 'count'])
        
        results_medium = execute_sql_query(query_risk_medium)
        df_medium = pd.DataFrame(results_medium, columns=['vendor', 'device_type_family', 'model', 'count'])
        
        return {
            "risk_dist": df_risk,
            "risk_critical": df_critical,
            "risk_high": df_high,
            "risk_medium": df_medium
        }
        
    except Exception as e:
        st.error(f"Error querying Risk stats: {str(e)}")
        return {
            "risk_dist": pd.DataFrame(),
            "risk_critical": pd.DataFrame(),
            "risk_high": pd.DataFrame(),
            "risk_medium": pd.DataFrame()
        }

@st.cache_data
def get_vulnerability_stats(
    region: Optional[str] = None,
    vertical: Optional[str] = None,
    organization: Optional[str] = None,
    industry: Optional[str] = None,
    account_status: Optional[str] = None,
    vendor: Optional[str] = None,
    device_category: Optional[str] = None,
    device_type_family: Optional[str] = None,
    device_subcategory: Optional[str] = None,
    model: Optional[str] = None,
    os_name: Optional[str] = None,
    mac_oui: Optional[str] = None
) -> Dict[str, pd.DataFrame]:
    """Fetch aggregated statistics for the Vulnerabilities tab.
    
    Queries the denormalized vulnerability table directly (no JOIN needed).
    Uses LATERAL VIEW EXPLODE to unnest vulnerability arrays.
    Splits Confirmed vs Potentially Relevant in pandas for efficiency.
    """
    try:
        # Build WHERE clause - applies directly to vulnerability table (has all filter fields)
        where_clause = build_where_clause(
            region, vertical, organization, industry, account_status, vendor, device_category,
            device_type_family, device_subcategory, model, os_name, mac_oui
        )
        
        # Single query: Filter by device attributes, EXPLODE array, aggregate by relevance + vuln
        # No JOIN needed - table already contains all filter fields
        query_vuln = f"""
        SELECT 
            effective_relevance,
            v.name as advisory_name,
            v.source_name,
            COUNT(*) as count
        FROM `s3-write-bucket`.sales_dashboard.displayable_devices_vulnerabilities
        LATERAL VIEW EXPLODE(vulnerabilities_list) exploded AS v
        WHERE {where_clause}
        GROUP BY effective_relevance, v.name, v.source_name
        ORDER BY effective_relevance, count DESC
        """
        
        results = execute_sql_query(query_vuln)
        df_all = pd.DataFrame(results, columns=['effective_relevance', 'advisory_name', 'source_name', 'count'])
        
        # Split results by effective_relevance in pandas
        df_confirmed_all = df_all[df_all['effective_relevance'] == 'Confirmed'].copy()
        df_potential_all = df_all[df_all['effective_relevance'] == 'Potentially Relevant'].copy()
        
        # Calculate totals (sum of all counts per category)
        total_confirmed = int(df_confirmed_all['count'].sum()) if not df_confirmed_all.empty else 0
        total_potential = int(df_potential_all['count'].sum()) if not df_potential_all.empty else 0
        
        # Get top 100 for display
        df_confirmed = df_confirmed_all[['advisory_name', 'source_name', 'count']].head(100).reset_index(drop=True)
        df_potential = df_potential_all[['advisory_name', 'source_name', 'count']].head(100).reset_index(drop=True)
        
        return {
            "vuln_confirmed": df_confirmed,
            "vuln_potential": df_potential,
            "vuln_confirmed_total": total_confirmed,
            "vuln_potential_total": total_potential
        }
        
    except Exception as e:
        st.error(f"Error querying Vulnerability stats: {str(e)}")
        return {
            "vuln_confirmed": pd.DataFrame(),
            "vuln_potential": pd.DataFrame(),
            "vuln_confirmed_total": 0,
            "vuln_potential_total": 0
        }

def build_where_clause(
    region, vertical, organization, industry, account_status, vendor, device_category,
    device_type_family, device_subcategory, model, os_name, mac_oui
):
    """Helper to build WHERE clause for stat functions."""
    def escape_sql_string(value: str) -> str:
        if value is None: return ""
        return str(value).replace("'", "''")
    
    where_conditions = []
    
    if region is not None:
        where_conditions.append(f"region = '{escape_sql_string(region)}'")
    if vertical is not None:
        where_conditions.append(f"vertical = '{escape_sql_string(vertical)}'")
    if organization is not None:
        where_conditions.append(f"organization = '{escape_sql_string(organization)}'")
    if industry is not None:
        where_conditions.append(f"industry = '{escape_sql_string(industry)}'")
    if account_status is not None:
        where_conditions.append(f"account_status = '{escape_sql_string(account_status)}'")
    if vendor is not None:
        where_conditions.append(f"vendor = '{escape_sql_string(vendor)}'")
    if device_category is not None:
        where_conditions.append(f"device_category = '{escape_sql_string(device_category)}'")
    if device_type_family is not None:
        where_conditions.append(f"device_type_family = '{escape_sql_string(device_type_family)}'")
    if device_subcategory is not None:
        where_conditions.append(f"device_subcategory = '{escape_sql_string(device_subcategory)}'")
    if model is not None:
        where_conditions.append(f"model = '{escape_sql_string(model)}'")
    if os_name is not None:
        where_conditions.append(f"os_name = '{escape_sql_string(os_name)}'")
    if mac_oui is not None:
        where_conditions.append(f"array_contains(mac_oui_list, '{escape_sql_string(mac_oui)}')")
    
    return " AND ".join(where_conditions) if where_conditions else "1=1"

def build_where_clause_with_alias(
    region, vertical, organization, industry, account_status, vendor, device_category,
    device_type_family, device_subcategory, model, os_name, mac_oui, alias='d'
):
    """Helper to build WHERE clause with table alias for JOIN queries."""
    def escape_sql_string(value: str) -> str:
        if value is None: return ""
        return str(value).replace("'", "''")
    
    where_conditions = []
    
    if region is not None:
        where_conditions.append(f"{alias}.region = '{escape_sql_string(region)}'")
    if vertical is not None:
        where_conditions.append(f"{alias}.vertical = '{escape_sql_string(vertical)}'")
    if organization is not None:
        where_conditions.append(f"{alias}.organization = '{escape_sql_string(organization)}'")
    if industry is not None:
        where_conditions.append(f"{alias}.industry = '{escape_sql_string(industry)}'")
    if account_status is not None:
        where_conditions.append(f"{alias}.account_status = '{escape_sql_string(account_status)}'")
    if vendor is not None:
        where_conditions.append(f"{alias}.vendor = '{escape_sql_string(vendor)}'")
    if device_category is not None:
        where_conditions.append(f"{alias}.device_category = '{escape_sql_string(device_category)}'")
    if device_type_family is not None:
        where_conditions.append(f"{alias}.device_type_family = '{escape_sql_string(device_type_family)}'")
    if device_subcategory is not None:
        where_conditions.append(f"{alias}.device_subcategory = '{escape_sql_string(device_subcategory)}'")
    if model is not None:
        where_conditions.append(f"{alias}.model = '{escape_sql_string(model)}'")
    if os_name is not None:
        where_conditions.append(f"{alias}.os_name = '{escape_sql_string(os_name)}'")
    if mac_oui is not None:
        where_conditions.append(f"array_contains({alias}.mac_oui_list, '{escape_sql_string(mac_oui)}')")
    
    return " AND ".join(where_conditions) if where_conditions else "1=1"

def get_uid_examples(
    region: Optional[str] = None,
    vertical: Optional[str] = None,
    organization: Optional[str] = None,
    industry: Optional[str] = None,
    account_status: Optional[str] = None,
    vendor: Optional[str] = None,
    device_category: Optional[str] = None,
    device_type_family: Optional[str] = None,
    device_subcategory: Optional[str] = None,
    model: Optional[str] = None,
    os_name: Optional[str] = None,
    mac_oui: Optional[str] = None
) -> pd.DataFrame:
    """Fetch UID examples with best classification data.
    
    Returns devices with the most populated classification fields:
    vendor, model, device_type_family, serial_number, sw_version, hw_version, product_code
    """
    try:
        where_clause = build_where_clause(
            region, vertical, organization, industry, account_status, vendor, device_category,
            device_type_family, device_subcategory, model, os_name, mac_oui
        )
        
        # Calculate classification score based on populated fields
        query = f"""
        SELECT 
            organization,
            uid,
            vendor,
            model,
            device_type_family,
            serial_number,
            sw_version,
            hw_version,
            product_code,
            (CASE WHEN vendor IS NOT NULL THEN 1 ELSE 0 END +
             CASE WHEN model IS NOT NULL THEN 1 ELSE 0 END +
             CASE WHEN device_type_family IS NOT NULL THEN 1 ELSE 0 END +
             CASE WHEN serial_number IS NOT NULL THEN 1 ELSE 0 END +
             CASE WHEN sw_version IS NOT NULL THEN 1 ELSE 0 END +
             CASE WHEN hw_version IS NOT NULL THEN 1 ELSE 0 END +
             CASE WHEN product_code IS NOT NULL THEN 1 ELSE 0 END) as classification_score
        FROM `s3-write-bucket`.sales_dashboard.displayable_devices
        WHERE {where_clause}
        ORDER BY classification_score DESC, organization, uid
        LIMIT 50
        """
        
        results = execute_sql_query(query)
        df = pd.DataFrame(results, columns=[
            'organization', 'uid', 'vendor', 'model', 'device_type_family',
            'serial_number', 'sw_version', 'hw_version', 'product_code', 'classification_score'
        ])
        return df
        
    except Exception as e:
        st.error(f"Error fetching UID examples: {str(e)}")
        return pd.DataFrame()

# Sidebar filters
st.sidebar.header("Filters")

# Create a placeholder for filter errors that we can clear later
filter_error_container = st.sidebar.empty()

# Get filter options
filter_options_result = get_filter_options()

if filter_options_result is None:
    # Show error if failed
    filter_error_container.error("⚠️ Filter options failed to load. Using defaults.")
    filter_options = {key: [] for key in [
        'region', 'vertical', 'organization', 'industry', 
        'vendor', 'device_category', 'device_type_family', 
        'device_subcategory', 'model', 'os_name', 'mac_oui'
    ]}
else:
    # Clear any previous error if successful
    filter_error_container.empty()
    filter_options = filter_options_result

# Use a form to batch filter changes - form only triggers rerun on submit
with st.sidebar.form("filters_form"):
    # Submit button at the top
    submitted = st.form_submit_button("🔄 Apply Filters & Refresh", use_container_width=True)
    
    st.subheader("Organization Filters")
    # Create filter dropdowns inside the form
    selected_region = st.selectbox(
        "Region",
        options=[None] + filter_options.get('region', []),
        format_func=lambda x: "All" if x is None else x
    )
    
    selected_vertical = st.selectbox(
        "Vertical",
        options=[None] + filter_options.get('vertical', []),
        format_func=lambda x: "All" if x is None else x
    )
    
    selected_organization = st.selectbox(
        "Organization",
        options=[None] + filter_options.get('organization', []),
        format_func=lambda x: "All" if x is None else x
    )
    
    selected_industry = st.selectbox(
        "Industry",
        options=[None] + filter_options.get('industry', []),
        format_func=lambda x: "All" if x is None else x
    )
    
    selected_account_status = st.selectbox(
        "Account Status",
        options=[None] + filter_options.get('account_status', []),
        format_func=lambda x: "All" if x is None else x
    )
    
    st.markdown("---")
    st.subheader("Device Filters")
    
    selected_device_category = st.selectbox(
        "Device Category",
        options=[None] + filter_options.get('device_category', []),
        format_func=lambda x: "All" if x is None else x
    )
    
    selected_device_subcategory = st.selectbox(
        "Device Subcategory",
        options=[None] + filter_options.get('device_subcategory', []),
        format_func=lambda x: "All" if x is None else x
    )
    
    selected_device_type_family = st.selectbox(
        "Device Type Family",
        options=[None] + filter_options.get('device_type_family', []),
        format_func=lambda x: "All" if x is None else x
    )
    
    selected_vendor = st.selectbox(
        "Vendor",
        options=[None] + filter_options.get('vendor', []),
        format_func=lambda x: "All" if x is None else x
    )
    
    selected_model = st.selectbox(
        "Model",
        options=[None] + filter_options.get('model', []),
        format_func=lambda x: "All" if x is None else x
    )
    
    selected_os_name = st.selectbox(
        "OS Name",
        options=[None] + filter_options.get('os_name', []),
        format_func=lambda x: "All" if x is None else x
    )
    
    selected_mac_oui = st.selectbox(
        "MAC OUI",
        options=[None] + filter_options.get('mac_oui', []),
        format_func=lambda x: "All" if x is None else x
    )

# Initialize session state to store last query result
if 'last_stats' not in st.session_state:
    st.session_state.last_stats = {
        "top_devices": pd.DataFrame(), 
        "subcategory": pd.DataFrame(),
        "category": pd.DataFrame(),
        "os_dist": pd.DataFrame(),
        "vendor_dist": pd.DataFrame(),
        "total_devices": 0,
        "source_coverage": pd.DataFrame(),
        "org_dist": pd.DataFrame(),
        "risk_dist": pd.DataFrame(),
        "risk_critical": pd.DataFrame(),
        "risk_high": pd.DataFrame(),
        "risk_medium": pd.DataFrame(),
        "vuln_confirmed": pd.DataFrame(),
        "vuln_potential": pd.DataFrame(),
        "vuln_confirmed_total": 0,
        "vuln_potential_total": 0
    }
if 'last_filters' not in st.session_state:
    st.session_state.last_filters = {}
if 'initial_load_done' not in st.session_state:
    st.session_state.initial_load_done = False
if 'vuln_needs_refresh' not in st.session_state:
    st.session_state.vuln_needs_refresh = True

# Auto-load on first run when all filters are "All" (None)
all_filters_all = (
    selected_region is None and
    selected_vertical is None and
    selected_organization is None and
    selected_industry is None and
    selected_account_status is None and
    selected_vendor is None and
    selected_device_category is None and
    selected_device_type_family is None and
    selected_device_subcategory is None and
    selected_model is None and
    selected_os_name is None and
    selected_mac_oui is None
)

# Query on form submit OR on initial load when all filters are "All"
should_query = submitted or (not st.session_state.initial_load_done and all_filters_all)

if should_query:
    # Store current filter values
    current_filters = {
        'region': selected_region,
        'vertical': selected_vertical,
        'organization': selected_organization,
        'industry': selected_industry,
        'account_status': selected_account_status,
        'vendor': selected_vendor,
        'device_category': selected_device_category,
        'device_type_family': selected_device_type_family,
        'device_subcategory': selected_device_subcategory,
        'model': selected_model,
        'os_name': selected_os_name,
        'mac_oui': selected_mac_oui
    }
    st.session_state.last_filters = current_filters
    
    # Scroll to top of page using JavaScript
    st.markdown(
        """
        <style>
            /* Force scroll to top via CSS animation trick */
            html {
                scroll-behavior: smooth;
            }
        </style>
        <script>
            // Multiple approaches to scroll to top
            var scrolled = false;
            function tryScroll() {
                if (scrolled) return;
                try {
                    // Method 1: Direct parent scroll
                    window.parent.scrollTo(0, 0);
                    // Method 2: Find Streamlit's main container
                    var containers = window.parent.document.querySelectorAll('.main, section.main, [data-testid="stAppViewContainer"]');
                    for (var i = 0; i < containers.length; i++) {
                        containers[i].scrollTop = 0;
                    }
                    // Method 3: Scroll the anchor into view
                    var anchor = window.parent.document.getElementById('top-anchor');
                    if (anchor) anchor.scrollIntoView(true);
                    scrolled = true;
                } catch(e) {}
            }
            tryScroll();
            setTimeout(tryScroll, 100);
            setTimeout(tryScroll, 500);
        </script>
        """,
        unsafe_allow_html=True
    )
    
    # Mark initial load as done
    if not st.session_state.initial_load_done:
        st.session_state.initial_load_done = True
    
    # Update state with Global stats
    with st.spinner("Loading Global data..."):
        stats_global = get_global_stats(
            region=selected_region,
            vertical=selected_vertical,
            organization=selected_organization,
            industry=selected_industry,
            account_status=selected_account_status,
            vendor=selected_vendor,
            device_category=selected_device_category,
            device_type_family=selected_device_type_family,
            device_subcategory=selected_device_subcategory,
            model=selected_model,
            os_name=selected_os_name,
            mac_oui=selected_mac_oui
        )
        st.session_state.last_stats.update(stats_global)
    
    # Update state with Risk stats (runs after Global is done)
    with st.spinner("Loading Risk data..."):
        stats_risk = get_risk_stats(
            region=selected_region,
            vertical=selected_vertical,
            organization=selected_organization,
            industry=selected_industry,
            account_status=selected_account_status,
            vendor=selected_vendor,
            device_category=selected_device_category,
            device_type_family=selected_device_type_family,
            device_subcategory=selected_device_subcategory,
            model=selected_model,
            os_name=selected_os_name,
            mac_oui=selected_mac_oui
        )
        st.session_state.last_stats.update(stats_risk)
    
    # Mark vulnerability data as needing refresh (will load lazily when tab is viewed)
    st.session_state.vuln_needs_refresh = True

# Get current stats (either newly fetched or from last run)
stats = st.session_state.last_stats
df_top = stats.get("top_devices", pd.DataFrame())
df_sub = stats.get("subcategory", pd.DataFrame())
df_cat = stats.get("category", pd.DataFrame())
df_os = stats.get("os_dist", pd.DataFrame())
df_vendor = stats.get("vendor_dist", pd.DataFrame())
total_devices = stats.get("total_devices", 0)
df_sources = stats.get("source_coverage", pd.DataFrame())
df_org_dist = stats.get("org_dist", pd.DataFrame())
df_risk = stats.get("risk_dist", pd.DataFrame())
df_critical = stats.get("risk_critical", pd.DataFrame())
df_high = stats.get("risk_high", pd.DataFrame())
df_medium = stats.get("risk_medium", pd.DataFrame())
# Note: Vulnerability data is loaded lazily in the Vulnerabilities tab

# Show data status (based on Global stats)
data_loaded = not df_top.empty or not df_sub.empty or not df_cat.empty

if not data_loaded:
    if not should_query and (not st.session_state.last_stats["top_devices"].empty):
        # Show last result with info message
        st.info("💡 Adjust filters and click 'Apply Filters & Refresh' to update the dashboard.")
    elif should_query:
        st.warning("No data found for the selected filters.")
        st.stop()
    else:
        st.info("👆 Select filters and click 'Apply Filters & Refresh' to load data.")
        st.stop()
else:
    # Show success message with filter info
    active_filters = sum([
        selected_region is not None,
        selected_vertical is not None,
        selected_organization is not None,
        selected_industry is not None,
        selected_vendor is not None,
        selected_device_category is not None,
        selected_device_type_family is not None,
        selected_device_subcategory is not None,
        selected_model is not None,
        selected_os_name is not None,
        selected_mac_oui is not None
    ])
    
    record_count_msg = "Dashboard data successfully loaded"
    if active_filters == 0:
        st.success(f"{record_count_msg} (All filters: showing all data)")
    else:
        st.success(f"{record_count_msg} ({active_filters} filter{'s' if active_filters > 1 else ''} applied)")
    
    # Create Tabs
    tab_global, tab_risk, tab_vuln = st.tabs(["Global", "Risk", "Vulnerabilities"])
    
    with tab_global:
        # --- VISUALIZATIONS ---
        
        # Total Device Count Metric - Centered with larger font
        st.markdown(
            f"""
            <div style="text-align: center; padding: 20px 0;">
                <p style="font-size: 1.2rem; color: #888; margin-bottom: 5px;">📊 Total Devices</p>
                <p style="font-size: 3rem; font-weight: bold; margin: 0;">{total_devices:,}</p>
            </div>
            """,
            unsafe_allow_html=True
        )
        st.divider()
        
        # Create two columns for pie charts
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Device Category Distribution")
            if not df_cat.empty:
                fig = go.Figure(data=[go.Pie(
                    labels=df_cat['device_category'].tolist(),
                    values=df_cat['count'].tolist(),
                    hole=0.3
                )])
                fig.update_layout(
                    height=400,
                    showlegend=True,
                    margin=dict(l=20, r=20, t=30, b=20)
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No category data available.")
    
        with col2:
            st.subheader("Device Subcategory Distribution")
            if not df_sub.empty:
                fig = go.Figure(data=[go.Pie(
                    labels=df_sub['device_subcategory'].tolist(),
                    values=df_sub['count'].tolist(),
                    hole=0.3
                )])
                fig.update_layout(
                    height=400,
                    showlegend=True,
                    margin=dict(l=20, r=20, t=30, b=20)
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No subcategory data available.")
        
        # Source Coverage - Horizontal Bar Chart
        st.divider()
        st.subheader("Data Sources Percentages")
        if not df_sources.empty and total_devices > 0:
            # Calculate percentage of devices that have each source
            df_sources_display = df_sources.copy()
            df_sources_display['percentage'] = (df_sources_display['device_count'] / total_devices * 100).round(1)
            
            # Sort by percentage descending
            df_sources_display = df_sources_display.sort_values('percentage', ascending=True)
            
            # Assign distinct colors to each bar (softer rainbow)
            num_sources = len(df_sources_display)
            colors = [
                f'hsl({int(i * 360 / num_sources)}, 55%, 45%)' 
                for i in range(num_sources)
            ]
            
            fig_sources = go.Figure(go.Bar(
                x=df_sources_display['percentage'],
                y=df_sources_display['source'],
                orientation='h',
                text=[f"{p}% ({c:,} devices)" for p, c in zip(df_sources_display['percentage'], df_sources_display['device_count'])],
                textposition='auto',
                marker=dict(
                    color=colors,
                    line=dict(color='white', width=1)
                )
            ))
            fig_sources.update_layout(
                xaxis_title="% of Devices",
                yaxis_title="",
                height=max(300, len(df_sources_display) * 50),  # Dynamic height based on number of sources
                margin=dict(l=0, r=0, t=20, b=40),
                xaxis=dict(range=[0, 105]),  # Allow some room for 100%+ labels
                yaxis=dict(tickfont=dict(size=14))  # Larger font for source labels
            )
            st.plotly_chart(fig_sources, use_container_width=True)
        else:
            st.info("No source data available.")
        
        # OS Distribution - Packed Bubble Chart
        st.divider()
        st.subheader("OS Distribution")
        if not df_os.empty:
            
            # Prepare data for spiral layout
            df_bubble = df_os.copy()
            df_bubble = df_bubble.sort_values('count', ascending=False).reset_index(drop=True)
            
            # Calculate percentages
            total_count = df_bubble['count'].sum()
            df_bubble['percent'] = (df_bubble['count'] / total_count * 100).round(1)
            
            # Spiral Coordinates Algorithm
            # This places the largest bubble in center (0,0) and spirals others out
            x_coords = []
            y_coords = []
            for i in range(len(df_bubble)):
                angle = 2.4 * i  # Golden angle approximation (radians)
                radius = 7 * math.sqrt(i)  # Spread factor
                x_coords.append(radius * math.cos(angle))
                y_coords.append(radius * math.sin(angle))
            
            # Boost small sizes for visibility (Logarithmic-like scaling for visualization)
            # This ensures small counts are visible bubbles, while large ones are still dominant
            # We use a base size + scaled count
            base_size = df_bubble['count'].max() * 0.05
            visual_sizes = df_bubble['count'] + base_size
            
            fig_bubble = go.Figure(go.Scatter(
                x=x_coords,
                y=y_coords,
                mode='markers+text',
                marker=dict(
                    size=visual_sizes,
                    sizemode='area',
                    # Larger sizeref = smaller bubbles. We lower it to make bubbles bigger.
                    sizeref=2.0 * visual_sizes.max() / (120**2), 
                    color=df_bubble.index, # Use Index (Integers) for colorscale
                    colorscale='Turbo', # Vibrant, varied scale
                    showscale=False
                ),
                text=[f"{row['os_name']}<br>{row['percent']}%" for _, row in df_bubble.iterrows()],
                textposition="middle center",
                textfont=dict(color='white', size=10, weight='bold'), # Ensure text is readable
                hoverinfo='text',
                hovertext=[f"{row['os_name']}: {row['count']} ({row['percent']}%)" for _, row in df_bubble.iterrows()]
            ))
            
            fig_bubble.update_layout(
                xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                height=600,
                margin=dict(l=0, r=0, t=20, b=0),
                plot_bgcolor='rgba(0,0,0,0)'
            )
            st.plotly_chart(fig_bubble, use_container_width=True)
            
        else:
            st.info("No OS data available.")

        # Table for top vendor, device_type_family, model
        st.divider()
        st.subheader("Top Devices by Vendor, Device Type Family, and Model")
        
        if not df_top.empty:
            # Filter for rows where all three fields are not null
            df_top_filtered = df_top[
                df_top['vendor'].notna() & 
                df_top['device_type_family'].notna() & 
                df_top['model'].notna()
            ].head(1000)
            
            if not df_top_filtered.empty:
                st.dataframe(
                    df_top_filtered,
                    use_container_width=True,
                    hide_index=True
                )
            else:
                st.info("No data available with all three fields populated.")
            
            # Create aggregated tables for Device Type Family and Vendor
            # Device Type Family counts (filter only where device_type_family is not null)
            df_dtype_counts = df_top[df_top['device_type_family'].notna()].groupby('device_type_family')['count'].sum().reset_index()
            df_dtype_counts = df_dtype_counts.sort_values('count', ascending=False).head(100)
            df_dtype_counts.columns = ['device_type_family', 'count']
            
            # Vendor counts (filter only where vendor is not null)
            df_vendor_counts = df_top[df_top['vendor'].notna()].groupby('vendor')['count'].sum().reset_index()
            df_vendor_counts = df_vendor_counts.sort_values('count', ascending=False).head(100)
            df_vendor_counts.columns = ['vendor', 'count']
            
            # Display two tables side by side
            st.divider()
            col_dtype, col_vendor = st.columns(2)
            
            with col_dtype:
                st.markdown("#### Top Device Type Families")
                if not df_dtype_counts.empty:
                    st.dataframe(df_dtype_counts, use_container_width=True, hide_index=True)
                else:
                    st.info("No device type family data available.")
            
            with col_vendor:
                st.markdown("#### Top Vendors")
                if not df_vendor_counts.empty:
                    st.dataframe(df_vendor_counts, use_container_width=True, hide_index=True)
                else:
                    st.info("No vendor data available.")
        else:
            st.info("No data available with all three fields (vendor, device_type_family, model) populated.")

        # Top 20 Vendors Pie Chart
        st.divider()
        st.subheader("Top 20 Vendors Distribution")
        
        if not df_vendor.empty:
            fig_vendor = go.Figure(data=[go.Pie(
                labels=df_vendor['vendor'].tolist(),
                values=df_vendor['count'].tolist(),
                hole=0.4
            )])
            fig_vendor.update_layout(
                height=500,
                showlegend=True,
                margin=dict(t=0, b=0, l=0, r=0)
            )
            st.plotly_chart(fig_vendor, use_container_width=True)
        else:
            st.info("No vendor data available.")
        
        # Top 20 Organizations - Lollipop Chart
        st.divider()
        st.subheader("Top 20 Organizations")
        
        if not df_org_dist.empty:
            # Get top 20 for display
            df_org_top20 = df_org_dist.head(20).copy()
            df_org_top20 = df_org_top20.sort_values('count', ascending=True)  # For horizontal chart
            
            # Create lollipop chart (scatter + line segments)
            fig_lollipop = go.Figure()
            
            # Add lines (stems)
            for i, row in df_org_top20.iterrows():
                fig_lollipop.add_trace(go.Scatter(
                    x=[0, row['count']],
                    y=[row['organization'], row['organization']],
                    mode='lines',
                    line=dict(color='#3f51b5', width=2),
                    showlegend=False,
                    hoverinfo='skip'
                ))
            
            # Add dots (lollipop heads)
            fig_lollipop.add_trace(go.Scatter(
                x=df_org_top20['count'],
                y=df_org_top20['organization'],
                mode='markers+text',
                marker=dict(size=12, color='#3f51b5'),
                text=df_org_top20['count'].apply(lambda x: f'{x:,}'),
                textposition='middle right',
                textfont=dict(size=11),
                showlegend=False,
                hovertemplate='%{y}<br>Count: %{x:,}<extra></extra>'
            ))
            
            fig_lollipop.update_layout(
                height=max(400, len(df_org_top20) * 30),
                margin=dict(l=0, r=60, t=20, b=40),
                xaxis_title="Device Count",
                yaxis_title="",
                yaxis=dict(tickfont=dict(size=12)),
                plot_bgcolor='rgba(0,0,0,0)'
            )
            st.plotly_chart(fig_lollipop, use_container_width=True)
            
            # CSV Download for all organizations
            st.download_button(
                label="📥 Download All Organizations (CSV)",
                data=df_org_dist.to_csv(index=False),
                file_name="organizations_device_count.csv",
                mime="text/csv"
            )
        else:
            st.info("No organization data available.")
        
        # UID Examples Section - Only shown when vendor or model filter is applied
        if selected_vendor is not None or selected_model is not None:
            st.divider()
            st.subheader("🔍 UID Examples (Best Classification)")
            st.caption("Shows devices with the most complete classification data (vendor, model, device_type_family, serial_number, sw_version, hw_version, product_code)")
            
            # Initialize session state for UID examples
            if 'uid_examples' not in st.session_state:
                st.session_state.uid_examples = None
            if 'uid_examples_filters' not in st.session_state:
                st.session_state.uid_examples_filters = None
            
            # Current filter signature to detect changes
            current_filter_sig = f"{selected_vendor}_{selected_model}_{selected_region}_{selected_organization}"
            
            # Button to fetch examples
            if st.button("📋 Get UID Examples", key="get_uid_examples"):
                with st.spinner("Fetching best classified device examples..."):
                    df_examples = get_uid_examples(
                        region=selected_region,
                        vertical=selected_vertical,
                        organization=selected_organization,
                        industry=selected_industry,
                        account_status=selected_account_status,
                        vendor=selected_vendor,
                        device_category=selected_device_category,
                        device_type_family=selected_device_type_family,
                        device_subcategory=selected_device_subcategory,
                        model=selected_model,
                        os_name=selected_os_name,
                        mac_oui=selected_mac_oui
                    )
                    st.session_state.uid_examples = df_examples
                    st.session_state.uid_examples_filters = current_filter_sig
            
            # Display examples if available
            if st.session_state.uid_examples is not None and not st.session_state.uid_examples.empty:
                df_display = st.session_state.uid_examples
                st.success(f"Found {len(df_display)} examples (sorted by classification score)")
                
                # Show the table
                st.dataframe(
                    df_display,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "classification_score": st.column_config.NumberColumn("Score", help="Number of populated classification fields (max 7)")
                    }
                )
                
                # CSV Download
                st.download_button(
                    label="📥 Download UID Examples (CSV)",
                    data=df_display.to_csv(index=False),
                    file_name="uid_examples.csv",
                    mime="text/csv",
                    key="download_uid_examples"
                )
            elif st.session_state.uid_examples is not None:
                st.info("No examples found for the current filters.")

    with tab_risk:
        st.subheader("Risk Score Distribution")
        
        if not df_risk.empty:
            # Color map for risk scores
            risk_colors = {
                'Critical': '#FF4B4B',  # Red
                'High': '#FFA500',      # Orange
                'Medium': '#FFFF00',    # Yellow
                'Low': '#00FF00',       # Green
                'None': '#808080'       # Grey
            }
            colors = [risk_colors.get(x, '#808080') for x in df_risk['risk_score']]
            
            fig = go.Figure(data=[go.Pie(
                labels=df_risk['risk_score'].tolist(),
                values=df_risk['count'].tolist(),
                hole=0.3,
                marker=dict(colors=colors)
            )])
            fig.update_layout(
                height=400,
                showlegend=True,
                margin=dict(l=20, r=20, t=30, b=20)
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No risk score data available.")
        
        st.divider()
        
        # Stacked tables for risk devices
        st.markdown("### 🔴 Critical Risk Devices")
        if not df_critical.empty:
            st.dataframe(df_critical, use_container_width=True, hide_index=True)
        else:
            st.info("No Critical risk devices found.")
            
        st.markdown("### 🟠 High Risk Devices")
        if not df_high.empty:
            st.dataframe(df_high, use_container_width=True, hide_index=True)
        else:
            st.info("No High risk devices found.")
            
        st.markdown("### 🟡 Medium Risk Devices")
        if not df_medium.empty:
            st.dataframe(df_medium, use_container_width=True, hide_index=True)
        else:
            st.info("No Medium risk devices found.")
    
    with tab_vuln:
        st.subheader("Vulnerability Analysis")
        
        # Lazy load vulnerability data when tab is viewed
        if st.session_state.vuln_needs_refresh:
            with st.spinner("Loading Vulnerability data..."):
                stats_vuln = get_vulnerability_stats(
                    region=selected_region,
                    vertical=selected_vertical,
                    organization=selected_organization,
                    industry=selected_industry,
                    account_status=selected_account_status,
                    vendor=selected_vendor,
                    device_category=selected_device_category,
                    device_type_family=selected_device_type_family,
                    device_subcategory=selected_device_subcategory,
                    model=selected_model,
                    os_name=selected_os_name,
                    mac_oui=selected_mac_oui
                )
                st.session_state.last_stats.update(stats_vuln)
                st.session_state.vuln_needs_refresh = False
        
        # Get vulnerability data from session state
        df_vuln_confirmed = st.session_state.last_stats.get("vuln_confirmed", pd.DataFrame())
        df_vuln_potential = st.session_state.last_stats.get("vuln_potential", pd.DataFrame())
        vuln_confirmed_total = st.session_state.last_stats.get("vuln_confirmed_total", 0)
        vuln_potential_total = st.session_state.last_stats.get("vuln_potential_total", 0)
        
        # Counters at the top
        col_confirmed, col_potential = st.columns(2)
        with col_confirmed:
            st.metric(label="🔴 Confirmed", value=f"{vuln_confirmed_total:,}")
        with col_potential:
            st.metric(label="🟡 Potentially Relevant", value=f"{vuln_potential_total:,}")
        
        st.divider()
        
        # Confirmed Vulnerabilities Table
        st.markdown("### 🔴 Confirmed Vulnerabilities")
        if not df_vuln_confirmed.empty:
            st.dataframe(df_vuln_confirmed, use_container_width=True, hide_index=True)
        else:
            st.info("No Confirmed vulnerabilities found.")
        
        st.divider()
        
        # Potentially Relevant Vulnerabilities Table
        st.markdown("### 🟡 Potentially Relevant Vulnerabilities")
        if not df_vuln_potential.empty:
            st.dataframe(df_vuln_potential, use_container_width=True, hide_index=True)
        else:
            st.info("No Potentially Relevant vulnerabilities found.")
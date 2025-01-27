import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import base64
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Duck theme colors - simplified palette
DUCK_COLORS = {
    'primary': '#4682B4',
    'secondary': '#FFA500',
    'accent': '#228B22'
}

# Page config - using wide layout
st.set_page_config(page_title="IoT Sensor Analytics", layout="wide", initial_sidebar_state="collapsed")

# Minimal CSS
st.markdown("""
    <style>
    .stButton>button {
        background-color: #4682B4;
        color: white;
        border: none;
    }
    div[data-testid="stMetricValue"] {
        font-size: 1.8rem;
        color: #4682B4;
    }
    .streamlit-expanderHeader {
        background-color: transparent !important;
    }
    </style>
""", unsafe_allow_html=True)

# Initialize connection
load_dotenv()

@st.cache_resource
def init_connection():
    conn=psycopg2.connect(
    dbname=os.getenv('DB'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD'),
    host=os.getenv('HOST'),
    port="5432"
    )
    cursor=conn.cursor()

    main_sql="""CREATE OR REPLACE view dbo.sensor_data AS
    SELECT 
        status,
        CAST(readings->>'rpm' AS FLOAT) as rpm,
        CAST(readings->>'temperature' AS FLOAT) as temperature,
        CAST(readings->>'vibration' AS FLOAT) as vibration,
        CAST(timestamp AS TIMESTAMP) as timestamp,
        machine_id,
        _airbyte_raw_id
    FROM dbo.sensor_data_flattened"""
    cursor.execute(main_sql)
    return conn
conn = init_connection()
cursor=conn.cursor()


# Header with single logo aligned with title
col1, col2 = st.columns([3, 1])

with col1:
    st.write("")  # Add some padding
    st.title("IoT Sensor Analytics")

with col2:
    # Custom CSS to align logo
    st.markdown("""
        <style>
        [data-testid="column"] {
            display: flex;
            align-items: center;
            justify-content: center;
        }
        </style>
    """, unsafe_allow_html=True)
    
    try:
        st.image("assets/logo.png", width=120)  # Increased size from 80 to 120
    except Exception as e:
        st.error(f"Could not load logo: {str(e)}")

# Sidebar controls with minimal styling
with st.sidebar:
    st.markdown("### Dashboard Controls")
    try:
        machines_query = "SELECT DISTINCT machine_id FROM dbo.sensor_data ORDER BY machine_id"
        cursor.execute(machines_query)
        machines =cursor.fetchall()
        machine_ids = [m[0] for m in machines]
        selected_machines = st.multiselect("Select Machines", machine_ids, default=machine_ids[:3])
    except psycopg2.Error as e:
        conn.rollback()  # Reset the connection to continue
        st.error(f"Query error: {str(e)}")

    st.markdown("### Time Range")
    time_ranges = {
        "Last Hour": timedelta(hours=1),
        "Last 4 Hours": timedelta(hours=4),
        "Last 12 Hours": timedelta(hours=12),
        "Last 24 Hours": timedelta(hours=24),
        "Last Week": timedelta(days=7)
    }
    selected_range = st.select_slider(
        "Select Time Range",
        options=list(time_ranges.keys()),
        value="Last 4 Hours"
    )
    try: 
        temp_query = """
            SELECT MIN(temperature) as min_temp, MAX(temperature) as max_temp 
            FROM dbo.sensor_data
        """
        cursor.execute(temp_query)
        temp_range = cursor.fetchone()
        temp_min, temp_max = st.slider(
            "Temperature Range (°C)",
            float(temp_range[0]), float(temp_range[1]),
            (float(temp_range[0]), float(temp_range[1]))
        )
    except psycopg2.Error as e:
        conn.rollback()  # Reset the connection to continue
        st.error(f"Query error: {str(e)}")

# Key Metrics with minimal decoration
try:
    metrics_query = f"""
        SELECT 
            AVG(temperature) as avg_temp,
            AVG(vibration) as avg_vib,
            AVG(rpm) as avg_rpm
        FROM dbo.sensor_data
        WHERE machine_id IN ({','.join([f"'{m}'" for m in selected_machines])})
        AND temperature BETWEEN {temp_min} AND {temp_max}
    """
    cursor.execute(metrics_query)
    metrics = cursor.fetchone()
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Temperature", f"{metrics[0]:.1f}°C")
    with col2:
        st.metric("Vibration", f"{metrics[1]:.2f}")
    with col3:
        st.metric("RPM", f"{metrics[2]:.0f}")

    # Charts section with minimal headers
    col1, col2 = st.columns(2)
except psycopg2.Error as e:
    conn.rollback()  # Reset the connection to continue
    st.error(f"Query error: {str(e)}")

with col1:
    st.subheader("Temperature Trends")

    # Correctly parameterized query
    query = """
    SELECT 
        timestamp,
        temperature,
        machine_id
    from dbo.sensor_data
    WHERE machine_id = ANY(%s)
    AND temperature BETWEEN %s AND %s
    ORDER BY timestamp DESC
    """

    try:
        # Execute query with parameters
        cursor.execute(query, (selected_machines, temp_min, temp_max))
        
        # Fetch results and construct DataFrame
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df_temp = pd.DataFrame(results, columns=columns)

        # Create Plotly line chart
        fig_temp = px.line(
            df_temp, x='timestamp', y='temperature', color='machine_id',
            color_discrete_sequence=[DUCK_COLORS['primary'], DUCK_COLORS['secondary'], DUCK_COLORS['accent']]
        )

        # Update chart layout
        fig_temp.update_layout(
            height=400,
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            margin=dict(l=20, r=20, t=20, b=20)
        )

        # Display chart in Streamlit
        st.plotly_chart(fig_temp, use_container_width=True)
    except psycopg2.Error as e:
        conn.rollback()  # Reset the connection to continue
        st.error(f"Query error: {str(e)}")
    except Exception as e:
        st.error(f"Error retrieving temperature trends: {str(e)}")

with col2:
    st.subheader("Sensor Analysis")
    chart_type = st.radio("View", ["Scatter", "3D"], horizontal=True)

    # Define query template
    query_template = """
    SELECT 
        vibration,
        rpm,
        temperature,
        machine_id
    from dbo.sensor_data
    WHERE machine_id = ANY(%s)
    AND temperature BETWEEN %s AND %s
    """

    try:
        # Execute query
        cursor.execute(query_template, (selected_machines, temp_min, temp_max))
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(results, columns=columns)

        if chart_type == "Scatter":
            # Create scatter plot
            fig_vib = px.scatter(
                df, x='rpm', y='vibration', color='temperature',
                color_continuous_scale=['blue', 'orange']
            )
            fig_vib.update_layout(
                height=400,
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                margin=dict(l=20, r=20, t=20, b=20)
            )
        else:
            # Create 3D scatter plot
            fig_vib = px.scatter_3d(
                df, x='rpm', y='vibration', z='temperature',
                color='machine_id',
                color_discrete_sequence=[DUCK_COLORS['primary'], DUCK_COLORS['secondary'], DUCK_COLORS['accent']]
            )
            fig_vib.update_layout(
                height=400,
                margin=dict(l=20, r=20, t=20, b=20)
            )

        # Display plot
        st.plotly_chart(fig_vib, use_container_width=True)
    except psycopg2.Error as e:
        conn.rollback()  # Reset the connection to continue
        st.error(f"Query error: {str(e)}")
    except Exception as e:
        st.error(f"Error loading sensor analysis data: {str(e)}")

# Anomaly Detection in expander
with st.expander("Anomaly Detection"):
    # Sensitivity slider
    std_dev_threshold = st.slider("Sensitivity (Standard Deviations)", 1.0, 5.0, 2.0, 0.1)

    # Construct parameterized anomaly query
    anomaly_query = f"""
    WITH stats AS (
    SELECT 
        machine_id,
        AVG(temperature) AS mean_temp,
        STDDEV(temperature) AS std_temp,
        AVG(vibration) AS mean_vib,
        STDDEV(vibration) AS std_vib
    FROM dbo.sensor_data
    WHERE machine_id = ANY(%s)
    GROUP BY machine_id
),
anomalies AS (
    SELECT 
        f.*,
        CASE 
            WHEN ABS(f.temperature - s.mean_temp) > %s * s.std_temp 
            OR ABS(f.vibration - s.mean_vib) > %s * s.std_vib
            THEN true 
            ELSE false 
        END AS is_anomaly
    FROM dbo.sensor_data f
    JOIN stats s ON f.machine_id = s.machine_id
    WHERE f.machine_id = ANY(%s)
)
SELECT *
FROM anomalies
WHERE is_anomaly = true
ORDER BY timestamp DESC
LIMIT 10;
    """

    try:
        # Execute the query with parameters
        cursor.execute(anomaly_query, (selected_machines, std_dev_threshold, std_dev_threshold, selected_machines))
        
        # Fetch the results
        anomalies = cursor.fetchall()
        
        # Convert to a DataFrame
        columns = [desc[0] for desc in cursor.description]  # Get column names
        anomalies_df = pd.DataFrame(anomalies, columns=columns)
        
        # Display anomalies
        if not anomalies_df.empty:
            st.dataframe(
                anomalies_df.style.highlight_max(
                    axis=0, subset=['temperature', 'vibration'], color='#4682B4'
                )
            )
        else:
            st.info("No anomalies detected with current settings.")
    except psycopg2.Error as e:
        conn.rollback()  # Reset the connection to continue
        st.error(f"Query error: {str(e)}")
    except Exception as e:
        st.error(f"Query error: {str(e)}")

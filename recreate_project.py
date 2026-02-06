
import os

PROJECT_FILES = {
    "app.py": r'''import streamlit as st
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine
from geoalchemy2 import Geometry, WKTElement
import os

st.set_page_config(page_title="Parquet to Postgres Importer", layout="wide")

st.title("Parquet to Postgres Importer")
st.markdown("Upload a Parquet file to import it into a PostgreSQL database. Spatial data is automatically detected.")

# Sidebar for Database Connection
st.sidebar.header("Database Connection")
db_host = st.sidebar.text_input("Host", value="192.168.1.2")
db_port = st.sidebar.text_input("Port", value="5432")
db_name = st.sidebar.text_input("Database Name", value="spatial")
db_user = st.sidebar.text_input("Username", value="hart")
db_password = st.sidebar.text_input("Password", type="password")
table_name = st.sidebar.text_input("Target Table Name", value="imported_data")
if_exists_opt = st.sidebar.selectbox("If Table Exists", ["fail", "replace", "append"], index=0)

# Main Area
input_method = st.radio("Choose Input Method", ["Upload File (Small Files)", "Local File Path (Large Files)"])

file_path = None
uploaded_file = None

if input_method == "Upload File (Small Files)":
    uploaded_file = st.file_uploader("Choose a Parquet/Geoparquet file", type=["parquet", "geoparquet"])
else:
    file_path_input = st.text_input("Enter absolute file path (e.g. /mnt/data/big_file.parquet)")
    if file_path_input and os.path.exists(file_path_input):
        file_path = file_path_input
    elif file_path_input:
        st.error("File not found.")

if st.button("Start Import"):
    if not db_name or not db_user or not db_password:
        st.error("Please fill in Database Connection details in the sidebar (Host, User, Password, DB Name).")
    else:
        # Determine source
        import pyarrow.parquet as pq
        
        source = None
        if input_method == "Upload File (Small Files)" and uploaded_file:
            source = uploaded_file
        elif input_method == "Local File Path (Large Files)" and file_path:
            source = file_path
        
        if source:
            db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
            
            try:
                engine = create_engine(db_url)
                
                # Ensure PostGIS extension is enabled
                from sqlalchemy import text
                with engine.connect() as conn:
                    conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis;"))
                    conn.commit()
                
                # --- PROBE STEP ---
                # Attempt to read schema/metadata using Geopandas to support GeoParquet spec robustly
                detected_geometry_col = None
                detected_crs = None
                is_spatial_file = False
                
                try:
                    # Read just 1 row to sniff metadata
                    # Note: read_parquet usually can read file paths or file-like objects
                    probe_gdf = gpd.read_parquet(source, rows=1) if isinstance(source, str) else gpd.read_parquet(source)
                    
                    if isinstance(probe_gdf, gpd.GeoDataFrame):
                        is_spatial_file = True
                        detected_geometry_col = probe_gdf.active_geometry_name or 'geometry'
                        detected_crs = probe_gdf.crs
                        st.success(f"Detected GeoParquet! Geometry Column: '{detected_geometry_col}', CRS: {detected_crs}")
                except Exception as probe_error:
                    # Not a geopandas readable file, or just standard parquet. 
                    # We continue and try to detect manually in loop.
                    # If it was a small upload file, we might have consumed the buffer.
                    if hasattr(source, 'seek'):
                        source.seek(0)
                
                # --- STREAMING STEP ---
                # Open Parquet file using PyArrow
                parquet_file = pq.ParquetFile(source)
                
                # Metadata detection
                st.info(f"Detected {parquet_file.num_row_groups} row groups. Total rows (approx): {parquet_file.metadata.num_rows}")
                
                # Iterate over batches
                batch_size = 50000 
                
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                rows_processed = 0
                total_rows = parquet_file.metadata.num_rows
                
                if total_rows and total_rows > 0:
                   pass
                else: 
                   # Fallback if metadata doesn't have rows
                   total_rows = 1 

                # State for first chunk handling
                is_first_chunk = True
                
                for batch in parquet_file.iter_batches(batch_size=batch_size):
                    try:
                        df_chunk = batch.to_pandas()
                    except ValueError:
                        # Fallback for KNIME or weird metadata where default conversion fails
                        df_chunk = batch.to_pandas(ignore_metadata=True)
                    except Exception as e:
                         # Try one last time ignoring metadata just in case
                         try:
                             df_chunk = batch.to_pandas(ignore_metadata=True)
                         except:
                             raise e
                    
                    chunk_is_spatial = False
                    chunk_to_write = df_chunk
                    
                    # Logic 1: We detected it was spatial via probe
                    if is_spatial_file and detected_geometry_col in df_chunk.columns:
                        chunk_is_spatial = True
                        # If the column came in as WKB/bytes (common in Arrow->Pandas), convert it
                        # If it came in as objects (already decoded?), ensure it's a GeoDataFrame
                        
                        col_data = df_chunk[detected_geometry_col]
                        
                        # Heuristic: convert to geometry objects if they look like bytes/WKB
                        if col_data.dtype == 'object' or col_data.dtype == 'category' or col_data.dtype == 'string':
                             # Try to convert from WKB if possible, or just pass to GeoDataFrame
                             import shapely.wkb
                             from shapely.errors import WKBReadingError
                             
                             # We can use gpd.from_wkb safely on bytes. 
                             # If it's already objects, we might need a verify. 
                             # Fast path: assume WKB if bytes.
                             # Check first element type
                             first_val = col_data.iloc[0] if len(col_data) > 0 else None
                             
                             if isinstance(first_val, bytes):
                                 geometry_objects = gpd.GeoSeries.from_wkb(col_data)
                             else:
                                 # Already objects or strings? 
                                 # If valid geometry objects, just passing them to GeoDataFrame is fine.
                                 geometry_objects = col_data

                             gdf_chunk = gpd.GeoDataFrame(df_chunk, geometry=geometry_objects)
                        else:
                             # Just cast
                             gdf_chunk = gpd.GeoDataFrame(df_chunk, geometry=detected_geometry_col)

                        # Apply detected CRS
                        if detected_crs:
                            gdf_chunk.set_crs(detected_crs, allow_override=True, inplace=True)
                        elif gdf_chunk.crs is None:
                            # Fallback
                            gdf_chunk.set_crs(epsg=4326, inplace=True)
                            
                        chunk_to_write = gdf_chunk

                    # Logic 2: Fallback manual detection (if probe failed but column exists)
                    elif 'geometry' in df_chunk.columns:
                        # Old logic fallback
                        chunk_is_spatial = True
                        gdf_chunk = gpd.GeoDataFrame(df_chunk, geometry=gpd.GeoSeries.from_wkb(df_chunk['geometry']))
                        if gdf_chunk.crs is None:
                             gdf_chunk.set_crs(epsg=4326, inplace=True)
                        chunk_to_write = gdf_chunk

                    # Write to DB
                    current_if_exists = if_exists_opt if is_first_chunk else 'append'
                    
                    if chunk_is_spatial:
                        chunk_to_write.to_postgis(table_name, engine, if_exists=current_if_exists, index=False)
                    else:
                        chunk_to_write.to_sql(table_name, engine, if_exists=current_if_exists, index=False)
                    
                    rows_processed += len(df_chunk)
                    is_first_chunk = False
                    
                    # Update progress
                    if total_rows > 1: # Avoid division by zero
                         progress = min(rows_processed / total_rows, 1.0)
                         progress_bar.progress(progress)
                    
                    status_text.text(f"Processed {rows_processed} rows...")
                
                st.success(f"Successfully imported {rows_processed} rows to table '{table_name}'!")
                
            except Exception as e:
                st.error(f"Error during import: {e}")
                st.exception(e)
        else:
            st.error("Please provide a valid file.")
''',

    "requirements.txt": r'''streamlit
pandas
geopandas
sqlalchemy
geoalchemy2
psycopg2-binary
pyarrow
''',

    "Dockerfile": r'''# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set the working directory to /app
WORKDIR /app

# Install system dependencies (required for geopandas/psycopg2)
RUN apt-get update && apt-get install -y \
    binutils \
    libproj-dev \
    gdal-bin \
    libgdal-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --trusted-host pypi.python.org -r requirements.txt

# Make port 8501 available to the world outside this container
EXPOSE 8501

# Run app.py when the container launches
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
''',

    "run.sh": r'''#!/bin/bash

# Navigate to the script's directory
cd "$(dirname "$0")"

# Check if venv exists
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
    source venv/bin/activate
    echo "Installing dependencies..."
    pip install -r requirements.txt
else
    source venv/bin/activate
fi

# Run the app
echo "Starting Parquet Importer..."
streamlit run app.py
''',
    
    "generate_data.py": r'''import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import numpy as np

# Create dummy data
data = {
    'id': range(10),
    'value': np.random.randn(10),
    'category': ['A', 'B'] * 5
}
df = pd.DataFrame(data)

# Create geometry
geometry = [Point(x, y) for x, y in zip(np.random.rand(10), np.random.rand(10))]
gdf = gpd.GeoDataFrame(df, geometry=geometry)

# Save to Parquet
# Geopandas saves geometry as WKB in parquet by default or uses geo-parquet spec if available.
# We will just use to_parquet which modern geopandas handles well.
gdf.to_parquet('dummy_spatial.parquet')

print("Created dummy_spatial.parquet")
'''
}

def recreate_files():
    print("Recreating project files...")
    for filename, content in PROJECT_FILES.items():
        with open(filename, 'w') as f:
            f.write(content)
        
        # Determine if it needs executable permissions
        if filename.endswith(".sh"):
            st = os.stat(filename)
            os.chmod(filename, st.st_mode | 0o111)
            
        print(f"Created/Updated {filename}")
    
    print("\nDone! You can run the app using:")
    print("  ./run.sh")

if __name__ == "__main__":
    recreate_files()

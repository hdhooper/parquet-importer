import streamlit as st
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
    if not db_name or not db_user:
        st.error("Please fill in Database Connection details in the sidebar.")
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
                
                # Open Parquet file using PyArrow
                parquet_file = pq.ParquetFile(source)
                
                # Metadata detection
                st.info(f"Detected {parquet_file.num_row_groups} row groups. Total rows (approx): {parquet_file.metadata.num_rows}")
                
                # Iterate over batches
                # Adjust batch_size based on memory constraints. 100,000 is usually a safe bet for tabular data.
                # For complex geometries, might need to be smaller.
                batch_size = 50000 
                
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                rows_processed = 0
                total_rows = parquet_file.metadata.num_rows
                
                # State for first chunk handling
                is_first_chunk = True
                
                for batch in parquet_file.iter_batches(batch_size=batch_size):
                    df_chunk = batch.to_pandas()
                    
                    # Detect Geometry
                    is_spatial = False
                    # Check for 'geometry' column or other common geo names if needed.
                    # Standard GeoParquet uses 'geometry'.
                    geo_col = 'geometry'
                    if geo_col in df_chunk.columns:
                        is_spatial = True
                        # If bytes, convert to geometry
                        if df_chunk[geo_col].dtype == 'object' or df_chunk[geo_col].dtype == 'category': 
                             # Heuristic: sometimes WKB comes as bytes. 
                             # Geopandas from_wkb can handle it.
                             import shapely.wkb
                             try:
                                 # We apply to a sample to check if valid WKB
                                 # But for speed we just assume if column is named geometry
                                 gdf_chunk = gpd.GeoDataFrame(df_chunk, geometry=gpd.from_wkb(df_chunk[geo_col]))
                             except:
                                 # Fallback if it's already geometry objects (unlikely from raw parquet read)
                                 gdf_chunk = gpd.GeoDataFrame(df_chunk, geometry=geo_col)
                        else:
                             # Should rely on Metadata but for raw parquet read often need explicit convert
                             pass
                        
                        # Use Metadata from detection if possible, else assume 4326 if missing
                        if gdf_chunk.crs is None:
                            gdf_chunk.set_crs(epsg=4326, inplace=True)
                            
                        chunk_to_write = gdf_chunk
                    else:
                        chunk_to_write = df_chunk

                    # Write to DB
                    # Logic:
                    # First chunk: Respect user 'if_exists' (fail, replace, append)
                    # Subsequent chunks: Force 'append'
                    
                    current_if_exists = if_exists_opt if is_first_chunk else 'append'
                    
                    if is_spatial:
                        chunk_to_write.to_postgis(table_name, engine, if_exists=current_if_exists, index=False)
                    else:
                        chunk_to_write.to_sql(table_name, engine, if_exists=current_if_exists, index=False)
                    
                    rows_processed += len(df_chunk)
                    is_first_chunk = False
                    
                    # Update progress
                    if total_rows:
                        progress = min(rows_processed / total_rows, 1.0)
                        progress_bar.progress(progress)
                    
                    status_text.text(f"Processed {rows_processed} / {total_rows} rows...")
                
                st.success(f"Successfully imported {rows_processed} rows to table '{table_name}'!")
                
            except Exception as e:
                st.error(f"Error during import: {e}")
                st.exception(e) # Show stack trace for debugging
        else:
            st.error("Please provide a valid file.")


import pandas as pd
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

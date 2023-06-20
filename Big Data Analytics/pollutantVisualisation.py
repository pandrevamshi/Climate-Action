
# Team Members - Deepika Gonela, Pandre Vamshi, Akshith Reddy Kota, Krishna Tej Alahari
# Data manipulation for visualizing data across a geomap, creating various visualizations based on the pollutant type and all countries
# Frameworks used here: ran locally
# Visualizing using https://www.datawrapper.de/maps/choropleth-map

import pandas as pd

df = pd.read_csv('/Users/nani/Library/CloudStorage/OneDrive-Personal/CourseWork/BigDataAnalytics/BigDataAnalytics/Assignments/Project/groupedData.csv/part-00000-51cd1ddc-2bc4-4c5a-8c53-e9ee67a40587-c000.csv')

# Fill empty fields with 0
df.fillna(value=0, inplace=True)

# dropped pollutants with all 0 values (no Data)
df = df.loc[:, (df != 0).any(axis=0)]

# Add iso codes to visualize
iso_codes = ['LTU', 'ITA', 'HUN', 'FRA', 'SWE', 'POL', 'CZE', 'DEU', 'CYP', 'SVN', 'PRT', 'FIN', 'NLD', 'EST', 'AUT', 'ESP', 'IRL', 'DNK', 'LVA', 'SVK', 'NOR', 'GBR', 'GRC', 'LUX', 'MLT', 'BEL', 'BGR', 'ROU', 'ISL', 'CHE', 'SRB', 'HRV']

# Add a column with ISO codes to the DataFrame for visualization
df = df.assign(iso_code=iso_codes)

# Ammonia Emissions
print(df[['iso_code', 'Ammonia (NH3)']])

df.to_csv('ammonia_emission.csv', index=False)

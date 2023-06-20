# -*- coding: utf-8 -*-
"""predictive_model.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1ZhY-qJSD8Pkbcg3qUwfwi2RWBzN2_O95

Team members: Deepika Gonela, Pandre Vamshi, Krishna Tej Alahari, Akshith Reddy Kota
General description of the code:
Part 1 (Emission Profiles)- This part of the code creates emission profiles for each of the facilities across the European Union
Part 2 (Similarity Search)- This part of the code performs locality sensitive hashing to find out similar facilities
Part 3 (Data preprocessing and merging)- This part of the code preprocesses the big data from all the weather stations. Then, we merge the emission profiles data with this big data
Part 4 (LSTM model)- This part of the code creates a predictive model that takes in emission profiles as input and predicts the change in climate variables
Part 5 (Correlation matrix)- This part of the code performs correlation on each of the pollutants in the emission profiles with the change in climate variables
Part 6 (Hypothesis Testing)- This part of the code performs hypothesis testing and finds out if one particular pollutant is significantly affecting the change in climate variables
Places where we have used the 2 dataframes:
1) Pyspark- Part 3 (Data preprocessing and merging)
2) Pytorch- Part 4 (LSTM Model) and Part 5 (Correlation Matrix)
Places where we used the concepts:
1) Locality sensitive hashing- Part 2
2) Creating a neural network- Part 4
3) Performing correlation- Part 5
4) Hypothesis Testing- Part 6
Type of system we have run our code on- Google Cloud DataProc Cluster
"""

# !pip install pyspark

import pandas as pd

from google.colab import drive
drive.mount('/content/drive')

"""## **Emission Profiles**"""

facilities = pd.read_csv('/content/drive/MyDrive/E-PRTR_database_v18_csv/dbo.PUBLISH_FACILITYREPORT.csv', encoding='latin-1')
facilityIDs = facilities['FacilityID'].unique()
facilitiesCount = facilityIDs.shape[0]

pollutants = pd.read_csv('/content/drive/MyDrive/E-PRTR_database_v18_csv/dbo.PUBLISH_POLLUTANTRELEASE.csv', encoding='latin-1')

# Merge Facilities and reporting year
mergePollutants = pd.merge(facilities, pollutants, on='FacilityReportID')
mergePollutants = mergePollutants.loc[:, ['FacilityID', 'FacilityReportID', 'PollutantReleaseAndTransferReportID', 'PollutantName', 'TotalQuantity', 'Lat', 'Long']]

years = pd.read_csv('/content/drive/MyDrive/E-PRTR_database_v18_csv/dbo.PUBLISH_POLLUTANTRELEASEANDTRANSFERREPORT.csv', encoding='latin-1')
years = years[years['ReportingYear'] >= 2017]

mergedYear = pd.merge(mergePollutants, years, on='PollutantReleaseAndTransferReportID')
mergedYear = mergedYear.loc[:, ['FacilityID', 'FacilityReportID', 'PollutantName', 'TotalQuantity', 'Lat', 'Long', 'CountryCode', 'CountryName']]
mergedYear['Index'] = mergedYear.reset_index().index

# Merge Facilities and pollutants
finalData = mergedYear.pivot(index = ['Index', 'FacilityReportID'], columns = 'PollutantName', values = 'TotalQuantity')
finalData = pd.merge(finalData, mergedYear[['FacilityReportID', 'FacilityID', 'Lat', 'Long', 'CountryCode', 'CountryName']], on = 'FacilityReportID')
finalData = finalData.drop(['FacilityReportID'], axis=1)
pollutantColumns = finalData.columns[:-5]

# Group by facility
finalData = finalData.groupby('FacilityID', as_index=False)
finalData = finalData.agg({**{col: 'sum' for col in pollutantColumns}, **{col: 'mean' for col in ['Lat', 'Long']}, **{col: 'first' for col in ['CountryCode', 'CountryName']}})
finalData = finalData.reset_index(drop=True)

finalData.to_csv('/content/drive/MyDrive/emissionProfilesData.csv', index = False)

print(pollutantColumns)

# Extracting Emission Profiles
emissionsProfiles = finalData.loc[:, pollutantColumns]
emissionsProfiles = emissionsProfiles.values.tolist()

"""## **Similarity Search**"""

# !pip install datasketch
# !pip install mmh3

from datasketch import MinHash, MinHashLSH
import mmh3

# Define the number of hash functions (k) and the signature length (b)
num_hash_functions = 240
signature_length = 240

# Create an empty signature matrix
signature_matrix = []
minhashes = []

# Create MinHash objects and compute the signatures for each list
for i, item in enumerate(emissionsProfiles):
    minhash = MinHash(num_perm=num_hash_functions)
    for value in item:
        minhash.update(str(value).encode('utf8'))
    minhashes.append((i, minhash))
    signature = minhash.digest()
    signature_matrix.append(signature[:signature_length])

print(len(signature_matrix), len(signature_matrix[0]))

from datasketch import MinHashLSHForest, MinHash

# LSH parameters
num_perm = 240  # Number of permutations for MinHash

# Create MinHash objects for all vectors
minhashes = []
for i, row in enumerate(signature_matrix):
    minhash = MinHash(num_perm=num_perm)
    for val in row:
        minhash.update(str(val).encode('utf-8'))
    minhashes.append((i, minhash))

# Build LSH Forest
forest = MinHashLSHForest(num_perm=num_perm)
for idx, minhash in minhashes:
    forest.add(idx, minhash)
forest.index()

# Retrieve similar groups
groups = {}
for idx, minhash in minhashes:
    similar_items = forest.query(minhash, k = 200)
    group_id = similar_items[0] % 200  # Assign the first similar item's group as the group ID
    if group_id in groups:
        groups[group_id].append(finalData.loc[[idx], ['FacilityID']].values[0][0])
    else:
        groups[group_id] = [finalData.loc[[idx], ['FacilityID']].values[0][0]]

# Print the groups
count = 0
groupSize = 0
groups_list = []
for group_id, vectors in groups.items():
    count += 1 
    groupSize += len(vectors)
    print(f"Group {group_id}: {vectors}")
    groups_list += vectors

"""## **Data Preprocessing and Merging using PySpark**"""

import pandas as pd
# sc.stop()
from pyspark import SparkContext

# Create a SparkContext
sc = SparkContext("local", "Weather")

# Load the CSV file
lines = sc.textFile("/content/drive/MyDrive/weather.csv")

# Store the header in a separate variable
header = lines.first()
lines = lines.filter(lambda line: line != header)

# Parse each line into a tuple of values
rdd = lines.map(lambda line: tuple(line.split(",")))

rdd=rdd.map(lambda x:tuple(x[1:]))
# filter out the unwanted field avg_snow_depth
filtered_rdd = rdd.map(lambda x: (x[0], x[1], x[2], x[3], float(x[4]), float(x[5]), float(x[6]), float(x[7]), float(x[8]), x[10]))

# create key-value pairs with name, year as key and (avg_temp, avg_max_temp, avg_prcp, count) as value
keyed_rdd = filtered_rdd.map(lambda x: ((x[0], x[1], x[2], x[5], x[6], x[9] ), (x[4], x[7], x[8], 1))) \
                        .reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1], x[2]+y[2], x[3]+y[3]))

# calculate averages for each name, year
avg_rdd = keyed_rdd.map(lambda x: (x[0][0], x[0][1], x[0][2], x[0][3], x[0][4], x[0][5], x[1][0]/x[1][3], x[1][1]/x[1][3], x[1][2]/x[1][3]))

grouped_rdd = avg_rdd.groupBy(lambda x: (x[0], x[1], x[3], x[4]))

# calculate the change in the last three elements over four years
change_rdd = grouped_rdd.flatMapValues(lambda x: sorted(list(x), key=lambda y: y[2])).mapValues(lambda x: (x[6], x[7], x[8])).groupByKey().mapValues(lambda x: (list(x)[-1][0] - list(x)[0][0], list(x)[-1][1] - list(x)[0][1], list(x)[-1][2] - list(x)[0][2]))

text = sc.textFile("/content/drive/MyDrive/emissionProfilesData.csv")

# Store the header in a separate variable
header = text.first()
text = text.filter(lambda line: line != header)

rdd = text.map(lambda line: tuple(line.split(",")))

rdd.take(1)

rdd1=rdd.map(lambda x:(x[0],(float(x[90]),float(x[91]))))

rdd2=change_rdd.map(lambda x:(x[0][0],(x[0][2],x[0][3])))

from math import radians, cos, sin, asin, sqrt
from pyspark import SparkContext

def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # Convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371  # Radius of earth in kilometers. Use 3956 for miles
    return c * r

# Create a Cartesian product of the two RDDs
combinations = rdd1.cartesian(rdd2)

# Calculate the distance between each facility and weather station
distances = combinations.map(lambda x: (x[0][0], x[1][0], haversine(x[0][1][1], x[0][1][0], x[1][1][1], x[1][1][0])))

# Group the combinations by facility and find the combination with the smallest distance
nearest_stations = distances.groupBy(lambda x: x[0]).map(lambda x: (x[0], min(x[1], key=lambda y: y[2])[1]))

# Create a new RDD with the facility and its nearest weather station
result = nearest_stations.map(lambda x: (x[0], x[1]))

ans=result.collect()

rdd1=rdd.map(lambda x:(x[0],(x[1:90])))

result=result.join(rdd1)

#Create an RDD that has the weather station name and all the climate variables
rdd2=change_rdd.map(lambda x:(x[0][0],(x[0][1],x[0][2],x[0][3],x[1][0],x[1][1],x[1][2])))

result1=result.map(lambda x:(x[1][0],(x[0],x[1][1])))

result1=result1.join(rdd2)

#Create a final RDD that has FacilityID as the key and values as emission profiles along with changes in average temperature, average maximum temperature and average precipitation
final=result1.map(lambda x:(x[1][0][0],(x[1][0][1],x[1][1][3],x[1][1][4],x[1][1][5])))

final.take(1)

from pandas.core.internals.blocks import new_block
from sklearn.model_selection import train_test_split

#Building data from the similarity groups
emprofile = []
weatherprof = []
for group_id,vectors in groups.items():
  req = final.filter(lambda x:int(x[0]) in vectors)
  #get data of all the similar groups
  li = req.collect()
  for x in li:
    new_l = (x[1][0])
    new_l = [float(x) for x in new_l]
    emprofile.append(new_l)
    weatherprof.append((x[1][1:]))

#sc.stop()

"""## **LSTM MODEL**"""

#importing required libraries
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, TensorDataset

class FacilityEmissionsLSTM(nn.Module):
    def __init__(self, input_size, hidden_size, num_layers, output_size):
        super(FacilityEmissionsLSTM, self).__init__()
        self.hidden_size = hidden_size
        self.num_layers = num_layers
        
        # LSTM layer
        self.lstm = nn.LSTM(input_size, hidden_size, num_layers, batch_first=True)
        
        # Fully connected layer for output
        self.fc = nn.Linear(hidden_size, output_size)

    def forward(self, x):
        batch_size = x.size(0)
        
        # Initialize hidden state and cell state
        h0 = torch.zeros(self.num_layers, batch_size, self.hidden_size).to(x.device)
        c0 = torch.zeros(self.num_layers, batch_size, self.hidden_size).to(x.device)
        
        # Forward pass through LSTM layer
        out, _ = self.lstm(x, (h0, c0))
        
        # Use only the last time step output
        out = self.fc(out[:, -1, :])
        
        return out

# Convert X and y into tensors
X = torch.tensor(emprofile, dtype=torch.float32)
y = torch.tensor(weatherprof, dtype=torch.float32)

# Reshape X to match the expected shape [num_samples, num_timesteps, num_features]
number_timestamp = 15  # Set the number of timestamps
X = X.unsqueeze(1).repeat(1, number_timestamp, 1)  # Add number_timestamp dimension

# Split your data into training and testing sets
train_ratio = 0.85
train_size = int(train_ratio * len(X))
train_X, test_X = X[:train_size], X[train_size:]
train_y, test_y = y[:train_size], y[train_size:]

# Create PyTorch DataLoader for batch processing
train_dataset = TensorDataset(train_X, train_y)
train_dataloader = DataLoader(train_dataset, batch_size=128, shuffle=True)

# Instantiate your LSTM network
input_size = X.shape[2]  # Number of input features
hidden_size = 40  # Number of LSTM units (hidden states)
num_layers = 5 # Number of LSTM layers
output_size = y.shape[1]  # Desired output size
net = FacilityEmissionsLSTM(input_size, hidden_size, num_layers, output_size)

# Define loss function and optimizer
criterion = nn.MSELoss()
optimizer = optim.Adam(net.parameters(), lr=0.01)

# Training loop
num_epochs = 15
for epoch in range(num_epochs):
    for batch_X, batch_y in train_dataloader:
        optimizer.zero_grad()
        
        # Forward pass
        output = net(batch_X)
        
        # Compute loss
        loss = criterion(output.squeeze(), batch_y)
        
        # Backward pass and optimization
        loss.backward()
        optimizer.step()

    # Print the training loss after each epoch
    print(f"Epoch {epoch+1}/{num_epochs}, Loss: {loss.item()}")

# Evaluation
net.eval()
with torch.no_grad():
    test_output = net(test_X)
    test_loss = criterion(test_output.squeeze(), test_y)

    # Convert the predicted output and ground truth to numpy arrays
    test_output_new = test_output.numpy()
    test_y_new = test_y.numpy()

    # Calculate accuracy based on a threshold
    threshold = 0.6
    correct = ((test_output_new - test_y_new) < threshold).all(axis=1).mean()
    accuracy = correct * 100
    print(f"Accuracy: {accuracy}%")

"""## **Correlation Matrix**"""

X = torch.tensor(emprofile)
y = torch.tensor(weatherprof)
# Convert tensors to NumPy arrays
X_np = X.numpy()
y_np = y.numpy()

# Concatenate X and y arrays
data = np.concatenate((X_np, y_np), axis=1)

# Calculate the correlation matrix using NumPy
correlation_matrix = np.corrcoef(data, rowvar=False)

# Keep only the last three columns
last_three_columns = correlation_matrix[:-3, -3:]

# Convert the last three columns to pandas DataFrame
df = pd.DataFrame(last_three_columns,index=None)
df.columns = ['Change_in_avg_temp','change_in_avg_max_temp','change_in_avg_precipitation']

#Get the facility attributes
df['Facility_Attributes'] = finalData.columns[1:-4]

cols = df.columns.tolist()

#create pandas dataframe with the required columns
cols = ['Facility_Attributes'] + cols[:cols.index('Facility_Attributes')] 
df = df[cols]

print(df)

# Write DataFrame to CSV file without index
df.to_csv('/content/drive/MyDrive/correlation.csv', index=False)

# !pip install networkx
import networkx as nx
import matplotlib.pyplot as plt

# Example list of groups

groups = emprofile[:20]

# Create an empty graph
G = nx.Graph()

# Add nodes and edges based on groups
for group in groups:
    G.add_nodes_from(group)
    G.add_edges_from([(group[i], group[j]) for i in range(len(group)) for j in range(i+1, len(group))])

# Plot the graph without showing node labels
pos = nx.spring_layout(G)  # Positions of nodes
nx.draw_networkx_nodes(G, pos, node_size=200, node_color='lightblue')
nx.draw_networkx_edges(G, pos)
plt.axis('off')

# Display the graph
plt.show()

# !pip install seaborn
import seaborn as sns

import seaborn as sns
import matplotlib.pyplot as plt



# Attribute names
# attributes = ['A', 'B', 'C', 'D']

# Create a heatmap
print(df.iloc[:, 1:])
sns.heatmap(correlation_matrix)

# Add labels and title
plt.xlabel('Facility Attributes')
plt.ylabel('Climate Change Variables')
# plt.title('Heatmap with Attribute Names')

# Display the plot
plt.show()

"""## **Group-based Correlation Matrix**"""

import numpy as np
from scipy.stats import pearsonr

count = 0
allValues = list()
for group_id, vectors in groups.items():
  if len(vectors) < 150:
    continue 
  emprofile = []
  weatherprof = []
  req = final.filter(lambda x:int(x[0]) in vectors)
  #get data of all the similar groups
  li = req.collect()
  for x in li:
    new_l = (x[1][0])
    new_l = [float(x) for x in new_l]


    emprofile.append(new_l)
    weatherprof.append((x[1][1:]))
  
  count= count+1
  # Convert tensors to NumPy arrays
  X= torch.tensor(emprofile)
  y = torch.tensor(weatherprof)
  X_np = X.numpy()
  y_np = y.numpy()

  # # Concatenate X and y arrays
  data = np.concatenate((X_np, y_np), axis=1)

  # Calculate the correlation matrix using NumPy
  correlation_matrix = np.corrcoef(data, rowvar=False)
  
  # Calculate p-values for correlation coefficients
  p_values = np.zeros_like(correlation_matrix)
  for i in range(correlation_matrix.shape[0]):
      for j in range(correlation_matrix.shape[1]):
          corr_coef, p_value = pearsonr(data[i], data[j])
          p_values[i, j] = p_value

  # Keep only the last three columns
  last_three_columns = correlation_matrix[:-3, -3:]
  p_last_three_columns = p_values[:-3, -3:]

  # Convert the last three columns to pandas DataFrame
  df1 = pd.DataFrame(last_three_columns,index=None)
  df2 = pd.DataFrame(p_last_three_columns,index=None)
  df1.columns = ['Change_in_avg_temp','change_in_avg_max_temp','change_in_avg_precipitation']
  df2.columns = ['Change_in_avg_temp','change_in_avg_max_temp','change_in_avg_precipitation']

  #Get the facility attributes
  df1['Facility_Attributes'] = finalData.columns[1:-4]
  df2['Facility_Attributes'] = finalData.columns[1:-4]
  cols = df1.columns.tolist()

  #create pandas dataframe with the required columns
  cols = ['Facility_Attributes'] + cols[:cols.index('Facility_Attributes')] 
  df1 = df1[cols]
  df2 = df2[cols]
  allValues.append([df1, df2])
  if count >= 5:
    break

hypothesisValues = list()
for i in range(len(allValues)):
  correlationMatrix, pValueMatrix = allValues[i]

  # Get the top 3 absolute values in the DataFrame
  top_3_abs_values = correlationMatrix.nlargest(5, correlationMatrix.columns)
  finalList = list()
  indices = top_3_abs_values.index.tolist()
  values = top_3_abs_values.values.tolist()
  for i in range(3):
    maxCol = -1
    maxVal = -99999999999999
    for j in range(3):
      if i == 0:
        if values[i][j] > maxVal:
          maxCol = j
          maxVal = values[i][j]
        else:
          maxVal = 999999999
      else:
        if i == 2:
          if -values[i][j] > maxVal:
            maxCol = j
            maxVal = values[i][j]
    finalList.append([pollutantColumns[indices[i]], df1.columns[maxCol], correlationMatrix.loc[indices[i], df1.columns[maxCol]], pValueMatrix.loc[indices[i], df1.columns[maxCol]]])
    hypothesisValues.append(finalList)

# Hypothesis Testing
alpha = 0.05
for i in range(3):
  pValue = hypothesisValues[0][i][3]
  if pValue < 0.05 and hypothesisValues[0][i][2] > 0:
    print("Observed correlation in the sample is statistically significant enough to say that increase in \"" + hypothesisValues[0][i][0] + "\", increases " + hypothesisValues[0][i][1])
  elif pValue < 0.05 and hypothesisValues[0][i][2] < 0:
    print("Observed correlation in the sample is statistically significant enough to say that increase in \"" + hypothesisValues[0][i][0] + "\", decreases " + hypothesisValues[0][i][1])
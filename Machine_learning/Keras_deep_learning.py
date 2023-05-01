import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder,MinMaxScaler
from keras.models import Sequential
from keras.layers import Dense, Dropout, BatchNormalization
from keras.optimizers import Adam


# Load the data into a dataframe
df = pd.read_csv('s3://ransomeware-data-poc/machine_learning_poc/linear_sector_train/000')

# Encode the categorical feature 'sector' using LabelEncoder
le = LabelEncoder()
df['sector'] = le.fit_transform(df['sector'])

# Extract month and year from 'month_end_date'
df['month'] = pd.to_datetime(df['breach_month_end_date']).dt.month
df['year'] = pd.to_datetime(df['breach_month_end_date']).dt.year

# Drop the original 'month_end_date' column
df.drop('breach_month_end_date', axis=1, inplace=True)

# Split the data into features (X) and labels (y)
X = df[['month', 'year', 'sector']]
y = df['count_victims']

# Scale the features using MinMaxScaler
scaler = MinMaxScaler()
X = scaler.fit_transform(X)
#X['sector'] = scaler.fit_transform(X[['sector']])

# Split the data into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Create a sequential model
model = Sequential()

# Add a dense layer with 32 input nodes, representing the three features
model.add(Dense(units=32, input_dim=3, activation='relu'))
model.add(BatchNormalization())
# Add a dropout layer for regularization
model.add(Dropout(0.4))
# Add another dense layer with 16 nodes
model.add(Dense(units=16, activation='relu'))
model.add(BatchNormalization())
# Add a dropout layer for regularization
model.add(Dropout(0.3))
# Add another dense layer with 8 nodes
model.add(Dense(units=8, activation='relu'))
model.add(BatchNormalization())
# Add a dropout layer for regularization
model.add(Dropout(0.2))
# Add another dense layer with 4 nodes
model.add(Dense(units=4, activation='relu'))
model.add(BatchNormalization())
# Add an output layer with 1 node, representing the predicted count_victims
model.add(Dense(units=1))

# Compile the model with mean squared error as the loss function and Adam optimizer with a lower learning rate
optimizer = Adam(lr=0.001)
model.compile(optimizer=optimizer, loss='mean_squared_error')

# Train the model
model.fit(X_train, y_train, epochs=500, batch_size=32)

# Evaluate the model on the testing data
loss = model.evaluate(X_test, y_test)
print('Test Loss:', loss)

# Make predictions
y_pred = model.predict(X_test)

# Round off the predicted count_victims to the nearest integer
y_pred = np.round(y_pred)

# Print sample predictions
print('Sample Predictions:')
for i in range(10):
    print('Actual:', y_test.iloc[i], 'Predicted:', y_pred[i][0])

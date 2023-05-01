import pandas as pd
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_squared_error
import pickle

df = pd.read_csv('/Users/depankarsarkar/Downloads/train_data_all')
#print(data.info())

df['breach_month_end_date'] = pd.to_datetime(df['breach_month_end_date'])

# Extract month and year from 'breach_month_end_date' column
df['month'] = df['breach_month_end_date'].dt.month
df['year'] = df['breach_month_end_date'].dt.year

# Drop the original 'breach_month_end_date' column
df.drop('breach_month_end_date', axis=1, inplace=True)

# Convert categorical columns to numerical using one-hot encoding
df = pd.get_dummies(df)
#print(df)

# Split the dataset into training and testing sets
X = df.drop('count_victims', axis=1)
y = df['count_victims']
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.1, random_state=42)

# Train the Ridge regression model with hyperparameter tuning
ridge_model = Ridge()

# Define the hyperparameters to be tuned
param_grid = {'alpha': [0.01, 0.1, 1, 10, 100]}

# Perform GridSearchCV to find the best hyperparameters
grid_search = GridSearchCV(ridge_model, param_grid, cv=5)
grid_search.fit(X_train, y_train)

# Get the best Ridge model with the optimal hyperparameters
best_ridge_model = grid_search.best_estimator_

# Train the best Ridge model with additional iterations
n_iterations = 10
for i in range(n_iterations):
    best_ridge_model.fit(X_train, y_train)

# Make predictions on the testing set
y_pred = best_ridge_model.predict(X_test)
#print(X_test)
#print(y_pred)

# Calculate mean squared error
mse = mean_squared_error(y_test, y_pred)
print("Mean Squared Error:", mse)

#data = pd.read_csv('/Users/depankarsarkar/Downloads/train_sector_region')
new_data=pd.read_csv('/Users/depankarsarkar/Downloads/pred_input_all')

new_data['breach_month_end_date'] = pd.to_datetime(new_data['breach_month_end_date'])

# Extract month and year from 'breach_month_end_date' column
new_data['month'] = new_data['breach_month_end_date'].dt.month
new_data['year'] = new_data['breach_month_end_date'].dt.year

# Drop the original 'breach_month_end_date' column
new_data.drop('breach_month_end_date', axis=1, inplace=True)


# Make sure the new data has the same columns as the data used for training
# Add any missing columns with value 0
new_data = pd.get_dummies(new_data)
X_train_columns = X_train.columns
print(X_train_columns)
for col in X_train_columns:
    if col not in new_data.columns:
        new_data[col] = 0

print(new_data.columns)

# Reorder the columns to match the order used for training
new_data = new_data[X_train_columns]

y_pred = best_ridge_model.predict(new_data)
#print(X_test)
print(y_pred)

# Calculate mean squared error
#mse = mean_squared_error(y_test, y_pred)
#print("Mean Squared Error:", mse)



with open('/Users/depankarsarkar/Desktop/machine_learning/ridge_model.pickle', 'wb') as f:
    pickle.dump(best_ridge_model, f)

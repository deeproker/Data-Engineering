import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
from sklearn.preprocessing import LabelEncoder

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

# Split the data into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Create a random forest regression model
model = RandomForestRegressor(n_estimators=100, random_state=42)

# Define the hyperparameter grid to search over
param_grid = {
    'max_depth': [3, 5, 7, 10],
    'min_samples_split': [2, 4, 6],
    'min_samples_leaf': [1, 2, 4],
}

# Perform grid search to find the optimal hyperparameters
grid_search = GridSearchCV(model, param_grid, cv=5, scoring='neg_mean_squared_error')
grid_search.fit(X_train, y_train)

# Print the best hyperparameters found
print('Best hyperparameters:', grid_search.best_params_)

# Train the model with the best hyperparameters found
best_model = RandomForestRegressor(**grid_search.best_params_, n_estimators=100, random_state=42)
best_model.fit(X_train, y_train)

# Make predictions on the testing data
y_pred = best_model.predict(X_test)
y_pred=np.floor(y_pred)-1

# Calculate the mean squared error
mse = mean_squared_error(y_test, y_pred)
print('Mean Squared Error:', mse)

# Print sample predictions
print('Sample Predictions:')
for i in range(10):
    print('Actual:', y_test.iloc[i], 'Predicted:', np.round(y_pred[i], 2))
    
def preprocessing_randomn(s3_path):
    # Load the data into a dataframe
    df = pd.read_csv(s3_path)

    # Encode the categorical feature 'sector' using LabelEncoder
    #df=df_input
    le = LabelEncoder()
    df['sector'] = le.fit_transform(df['sector'])

    # Extract month and year from 'month_end_date'
    df['month'] = pd.to_datetime(df['breach_month_end_date']).dt.month
    df['year'] = pd.to_datetime(df['breach_month_end_date']).dt.year

    # Drop the original 'month_end_date' column
    df.drop('breach_month_end_date', axis=1, inplace=True)

    # Split the data into features (X) and labels (y)
    X = df[['month', 'year', 'sector']]
    return X
  
  
XXX= preprocessing_randomn('s3://ransomeware-data-poc/machine_learning_poc/linear_sector_test/000')
yyy_pred = best_model.predict(XXX)
yyy_pred=np.floor(yyy_pred-1)
yyy_pred[yyy_pred < 0] = 0
print(yyy_pred)
print(yyy_pred.sum())
X_test_input = pd.read_csv('s3://ransomeware-data-poc/machine_learning_poc/linear_sector_test/000')
X_test_input['count_victims']=yyy_pred
print(X_test_input)

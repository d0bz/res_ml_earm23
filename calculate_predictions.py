import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.impute import SimpleImputer
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.ensemble import RandomForestRegressor
from datetime import datetime

start_time = datetime.now()
print("Script start:", start_time.strftime("%H:%M:%S"))


# Read in prepared csv that is generated by prepare_data.py
data = pd.read_csv(
    'data/prepared/output.csv',
    sep=',',
    decimal='.',
    parse_dates=['date']
)

# manual splitting by date for debugging purpose only
#data = data[data['date'] < '2023-11-01 12:00:00']
print("Loaded data length: {}".format(data.size))


# Scale features
scaler = StandardScaler()
features = ['bat_power', 'pv_power', 'ewh_power', 'grid_power', 'price_w_min']
data[features] = scaler.fit_transform(data[features])

# Encode categorical data
data['date_time'] = pd.to_datetime(data['date'])

# Extracting different time-based features
data['day_of_week'] = data['date_time'].dt.dayofweek
data['hour'] = data['date_time'].dt.hour
data['month'] = data['date_time'].dt.month

encoder = OneHotEncoder()
data = pd.concat([data, pd.get_dummies(data['day_of_week'], prefix='dow')], axis=1)

categorical_features = encoder.fit_transform(data[['day_of_week']])
categorical_features_df = pd.DataFrame(categorical_features, columns=['day_of_week'])

# Concatenate encoded features back to the main dataframe
data = pd.concat([data, categorical_features_df], axis=1)

# split data by allocating test_size for validating 0.2=20%, all left is for training
train_data, test_data, train_labels, test_labels = train_test_split(data, data[features], test_size=0.2, random_state=42)

model = RandomForestRegressor(n_estimators=100)
model.fit(train_data[features], train_data['cost_savings'])  # Assuming 'cost_savings' is the target variable

predicted_savings = model.predict(test_data[features])

results = pd.DataFrame({
    'Date': test_data['date'],
    'Actual': test_data['cost_savings'],
    'Predicted': predicted_savings
})

results.to_csv('data/prepared/predicted_results.csv', index=False)


mse = mean_squared_error(test_data['cost_savings'], predicted_savings)
r2 = r2_score(test_data['cost_savings'], predicted_savings)
print(f'MSE: {mse}')
print("R^2 Score:", r2)

end_time = datetime.now()
duration = end_time - start_time
duration_in_seconds = duration.total_seconds()
print("Script end: {}, Duration: {}".format(end_time.strftime("%H:%M:%S"), duration_in_seconds))


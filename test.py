import pandas as pd
import joblib
import os
from tensorflow.keras.models import load_model

# 1. LOAD EVERYTHING
# We need to load the data from your project folders
X_test = pd.read_csv('data/final/X_test.csv')
y_test = pd.read_csv('data/final/y_test.csv')
y_scaler = joblib.load('data/final/y_scaler.pkl')

# Load your trained model (adjust the filename if yours is different)
model = load_model('models/hadt.pkl')

# 2. GENERATE PREDICTIONS
predictions = model.predict(X_test)

# 3. CONVERT BACK TO REAL PRICES
# This turns the 0.123 scaled values back into actual Pesos
actual_prices = y_scaler.inverse_transform(y_test.values).flatten()
predicted_prices = y_scaler.inverse_transform(predictions).flatten()

# 4. BUILD THE TABLE
# We start with your factors (Factor 1, Factor 2, etc.)
results_df = X_test.copy()

# Add the prices right at the beginning
results_df.insert(0, 'Actual Price', actual_prices)
results_df.insert(1, 'Predicted Price', predicted_prices)

# Add the 'Test No.' or 'Date' column at the very start (Index 0)
if 'Month' in results_df.columns and 'Year' in results_df.columns:
    # If your CSV already has date columns, we just move them or leave them
    print("Detected Date columns in data.")
else:
    # If no dates, we add a sequential Test Number
    results_df.insert(0, 'Test No.', range(1, len(results_df) + 1))

# 5. SAVE TO CSV
os.makedirs('results', exist_ok=True)
output_path = 'results/test_results_table.csv'
results_df.to_csv(output_path, index=False)

print(f"--- Process Complete ---")
print(f"Table saved to: {output_path}")
print(results_df.head()) # Shows you the first 5 rows in the terminal
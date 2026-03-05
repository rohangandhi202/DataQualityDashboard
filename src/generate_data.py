import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os

# Set random seed for reproducibility
np.random.seed(42)
random.seed(42)

def generate_sample_data(num_rows=1000):
    """
    Generate synthetic transaction data with intentional quality issues
    to test our Great Expectations validations
    """
    
    # Create date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    dates = [start_date + timedelta(days=random.random() * 30) for _ in range(num_rows)]
    
    # Generate data
    data = {
        'transaction_id': range(1, num_rows + 1),
        'user_id': np.random.randint(1000, 2000, num_rows),
        'product_name': np.random.choice(
            ['Laptop', 'Mouse', 'Keyboard', 'Monitor', 'Headphones', 'Desk', 'Chair'],
            num_rows
        ),
        'amount': np.random.uniform(10, 1000, num_rows).round(2),
        'status': np.random.choice(['completed', 'pending', 'failed', 'cancelled'], num_rows),
        'transaction_date': dates,
        'email': [f'user_{i}@example.com' for i in range(num_rows)],
    }
    
    df = pd.DataFrame(data)
    
    # Introduce intentional data quality issues (20% of rows)
    num_issues = int(num_rows * 0.2)
    issue_indices = np.random.choice(df.index, num_issues, replace=False)
    
    # Add some nulls
    df.loc[issue_indices[:num_issues//3], 'email'] = None
    
    # Add some invalid amounts (negative)
    amount_indices = issue_indices[num_issues//3:2*num_issues//3]
    df.loc[amount_indices, 'amount'] = -abs(np.random.uniform(10, 100, len(amount_indices)))
    
    # Add some duplicate rows
    duplicates = df.sample(n=num_issues//3)
    df = pd.concat([df, duplicates], ignore_index=True)
    
    # Add some invalid statuses
    status_indices = issue_indices[2*num_issues//3:]
    df.loc[status_indices, 'status'] = np.random.choice(['unknown', 'pending_review', None], len(status_indices))
    
    return df

def main():
    # Ensure data directory exists
    data_dir = '../data'
    os.makedirs(data_dir, exist_ok=True)
    
    # Generate and save data
    df = generate_sample_data(1000)
    
    # Save to CSV
    csv_path = os.path.join(data_dir, 'sample_transactions.csv')
    df.to_csv(csv_path, index=False)
    
    print(f"✓ Generated {len(df)} rows of sample data")
    print(f"✓ Saved to: {csv_path}")
    print(f"\nData preview:")
    print(df.head(10))
    print(f"\nData shape: {df.shape}")
    print(f"\nData types:\n{df.dtypes}")
    print(f"\nNull values:\n{df.isnull().sum()}")

if __name__ == '__main__':
    main()
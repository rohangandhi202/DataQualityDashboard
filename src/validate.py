import pandas as pd
import json
from datetime import datetime
from sqlalchemy import create_engine, text
from config import DATABASE_URL
import logging

# Custom JSON encoder to handle NumPy and other non-serializable types
class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        # Handle NumPy scalar types
        if hasattr(obj, 'item'):
            return obj.item()
        # Handle datetime
        if isinstance(obj, datetime):
            return obj.isoformat()
        # Handle standard bool
        if isinstance(obj, bool):
            return bool(obj)
        return super().default(obj)

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

def create_connection():
    """Create a database connection"""
    try:
        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return engine
    except Exception as e:
        logger.error(f"✗ Failed to connect: {e}")
        raise

def run_expectations(engine):
    """
    Run data quality expectations on the transactions table
    Returns a dict with validation results
    """
    results = []
    
    logger.info("Running data quality validations...\n")
    
    try:
        with engine.connect() as conn:
            # Read data into pandas for easier validation
            df = pd.read_sql("SELECT * FROM transactions", conn)
            
            # 1. Check table exists and has rows
            expectation = "table_row_count_between_1_and_10000"
            row_count = len(df)
            passed = 1 <= row_count <= 10000
            results.append({
                'expectation': expectation,
                'passed': passed,
                'message': f"Row count: {row_count}" if passed else f"Row count {row_count} outside range"
            })
            logger.info(f"{'✓' if passed else '✗'} {expectation}: {row_count} rows")
            
            # 2. Check for required columns
            expectation = "columns_exist"
            required_cols = ['transaction_id', 'user_id', 'amount', 'status', 'email']
            passed = all(col in df.columns for col in required_cols)
            results.append({
                'expectation': expectation,
                'passed': passed,
                'message': "All required columns present" if passed else f"Missing columns"
            })
            logger.info(f"{'✓' if passed else '✗'} {expectation}: {required_cols}")
            
            # 3. Check transaction_id is unique (no duplicates)
            expectation = "transaction_id_unique"
            duplicates = df['transaction_id'].duplicated().sum()
            passed = duplicates == 0
            results.append({
                'expectation': expectation,
                'passed': passed,
                'message': f"No duplicates" if passed else f"{duplicates} duplicate transaction_ids"
            })
            logger.info(f"{'✓' if passed else '✗'} {expectation}: {duplicates} duplicates found")
            
            # 4. Check status values are valid
            expectation = "status_values_valid"
            valid_statuses = {'completed', 'pending', 'failed', 'cancelled'}
            invalid_statuses = set(df['status'].unique()) - valid_statuses
            invalid_count = df[~df['status'].isin(valid_statuses)].shape[0]
            passed = invalid_count == 0
            results.append({
                'expectation': expectation,
                'passed': passed,
                'message': "All statuses valid" if passed else f"{invalid_count} invalid statuses: {invalid_statuses}"
            })
            logger.info(f"{'✓' if passed else '✗'} {expectation}: {invalid_count} invalid statuses")
            
            # 5. Check amount is positive (or null)
            expectation = "amount_positive_or_null"
            negative_amounts = (df['amount'] < 0).sum()
            passed = negative_amounts == 0
            results.append({
                'expectation': expectation,
                'passed': passed,
                'message': "No negative amounts" if passed else f"{negative_amounts} negative amounts found"
            })
            logger.info(f"{'✓' if passed else '✗'} {expectation}: {negative_amounts} negative values")
            
            # 6. Check email format (simple check)
            expectation = "email_format_valid"
            invalid_emails = df[df['email'].notna() & (~df['email'].str.contains('@', na=False))].shape[0]
            passed = invalid_emails == 0
            results.append({
                'expectation': expectation,
                'passed': passed,
                'message': "All emails valid" if passed else f"{invalid_emails} invalid email formats"
            })
            logger.info(f"{'✓' if passed else '✗'} {expectation}: {invalid_emails} invalid formats")
            
            # 7. Check user_id is not null
            expectation = "user_id_not_null"
            null_user_ids = df['user_id'].isnull().sum()
            passed = null_user_ids == 0
            results.append({
                'expectation': expectation,
                'passed': passed,
                'message': "No null user_ids" if passed else f"{null_user_ids} null user_ids"
            })
            logger.info(f"{'✓' if passed else '✗'} {expectation}: {null_user_ids} nulls")
            
            # Calculate overall quality score
            passed_checks = sum(1 for r in results if r['passed'])
            total_checks = len(results)
            quality_score = (passed_checks / total_checks) * 100
            
            logger.info(f"\n{'='*50}")
            logger.info(f"Data Quality Score: {quality_score:.1f}% ({passed_checks}/{total_checks} checks passed)")
            logger.info(f"{'='*50}\n")
            
            return {
                'timestamp': datetime.now(),
                'total_checks': total_checks,
                'passed_checks': passed_checks,
                'quality_score': quality_score,
                'expectations': results
            }
            
    except Exception as e:
        logger.error(f"✗ Error running validations: {e}")
        raise

def store_results(engine, validation_results):
    """Store validation results in the database"""
    try:
        with engine.connect() as conn:
            for expectation in validation_results['expectations']:
                # Use custom encoder to handle bool type
                details_json = json.dumps(expectation, cls=CustomEncoder)
                
                conn.execute(text("""
                    INSERT INTO validation_results 
                    (validation_timestamp, expectation_name, status, passed_checks, total_checks, details)
                    VALUES (:timestamp, :name, :status, :passed, :total, :details)
                """), {
                    'timestamp': validation_results['timestamp'],
                    'name': expectation['expectation'],
                    'status': 'PASSED' if expectation['passed'] else 'FAILED',
                    'passed': validation_results['passed_checks'],
                    'total': validation_results['total_checks'],
                    'details': details_json
                })
            conn.commit()
        logger.info("✓ Results stored in validation_results table")
    except Exception as e:
        logger.error(f"✗ Failed to store results: {e}")
        raise

def main():
    logger.info("Data Quality Validation Pipeline\n")
    
    try:
        # Create connection
        engine = create_connection()
        logger.info("✓ Connected to PostgreSQL\n")
        
        # Run validations
        results = run_expectations(engine)
        
        # Store results
        store_results(engine, results)
        
        logger.info("✓ Validation pipeline complete!")
        
    except Exception as e:
        logger.error(f"✗ Validation pipeline failed: {e}")
        exit(1)

if __name__ == '__main__':
    main()
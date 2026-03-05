import pandas as pd
from sqlalchemy import create_engine, text
from config import DATABASE_URL, SAMPLE_DATA_PATH
import logging
import os

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

def create_connection():
    """Create a database connection"""
    try:
        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("✓ Connected to PostgreSQL")
        return engine
    except Exception as e:
        logger.error(f"✗ Failed to connect to database: {e}")
        logger.error("Make sure PostgreSQL is running and credentials in config.py are correct")
        raise

def create_tables(engine):
    """Create necessary tables in PostgreSQL"""
    try:
        with engine.connect() as conn:
            # Create transactions table
            conn.execute(text("""
                DROP TABLE IF EXISTS transactions CASCADE;
                CREATE TABLE transactions (
                    transaction_id INT PRIMARY KEY,
                    user_id INT NOT NULL,
                    product_name VARCHAR(100),
                    amount NUMERIC(10, 2),
                    status VARCHAR(50),
                    transaction_date TIMESTAMP,
                    email VARCHAR(100),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """))
            
            # Create validation results table
            conn.execute(text("""
                DROP TABLE IF EXISTS validation_results CASCADE;
                CREATE TABLE validation_results (
                    id SERIAL PRIMARY KEY,
                    validation_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    expectation_name VARCHAR(255),
                    status VARCHAR(20),
                    passed_checks INT,
                    total_checks INT,
                    failure_reason TEXT,
                    details JSONB
                );
            """))
            
            conn.commit()
        logger.info("✓ Tables created successfully")
    except Exception as e:
        logger.error(f"✗ Failed to create tables: {e}")
        raise

def load_data(engine):
    """Load CSV data into PostgreSQL"""
    try:
        if not os.path.exists(SAMPLE_DATA_PATH):
            logger.error(f"✗ Data file not found: {SAMPLE_DATA_PATH}")
            logger.error("Run generate_data.py first to create sample data")
            return None
            
        df = pd.read_csv(SAMPLE_DATA_PATH)
        
        # Convert transaction_date to datetime
        df['transaction_date'] = pd.to_datetime(df['transaction_date'])
        
        # Load into PostgreSQL
        df.to_sql('transactions', engine, if_exists='append', index=False)
        logger.info(f"✓ Loaded {len(df)} rows into transactions table")
        
        return df
    except Exception as e:
        logger.error(f"✗ Failed to load data: {e}")
        raise

def verify_data(engine):
    """Verify data was loaded correctly"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM transactions;"))
            row_count = result.scalar()
            
            result = conn.execute(text("SELECT * FROM transactions LIMIT 5;"))
            columns = [col[0] for col in result.cursor.description]
            sample = result.fetchall()
            
            logger.info(f"✓ Database contains {row_count} rows")
            logger.info(f"✓ Columns: {', '.join(columns)}")
            logger.info(f"✓ Sample rows retrieved successfully")
    except Exception as e:
        logger.error(f"✗ Failed to verify data: {e}")
        raise

def main():
    logger.info("Starting data ingestion...\n")
    
    try:
        # Create connection
        engine = create_connection()
        
        # Create tables
        create_tables(engine)
        
        # Load data
        load_data(engine)
        
        # Verify
        verify_data(engine)
        
        logger.info("\n✓ Data ingestion complete!")
    except Exception as e:
        logger.error(f"\n✗ Data ingestion failed: {e}")
        exit(1)

if __name__ == '__main__':
    main()
"""
Verify data integrity and quality in PostgreSQL.
"""
from database_connection import DatabaseConnection
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataVerifier:
    """Run data quality checks."""
    
    @staticmethod
    def run_all_checks():
        """Run all verification checks."""
        checks = [
            DataVerifier.check_row_counts,
            DataVerifier.check_nulls,
            DataVerifier.check_duplicates,
            DataVerifier.check_data_ranges,
            DataVerifier.check_fraud_logic,
            DataVerifier.check_partitions,
        ]
        
        logger.info("\n" + "="*70)
        logger.info("DATA QUALITY VERIFICATION")
        logger.info("="*70)
        
        passed = 0
        failed = 0
        
        for check in checks:
            try:
                result = check()
                if result:
                    passed += 1
                    logger.info(f"✅ {check.__name__} PASSED")
                else:
                    failed += 1
                    logger.warning(f"❌ {check.__name__} FAILED")
            except Exception as e:
                failed += 1
                logger.error(f"❌ {check.__name__} ERROR: {e}")
        
        logger.info("\n" + "="*70)
        logger.info(f"SUMMARY: {passed} passed, {failed} failed")
        logger.info("="*70)
    
    @staticmethod
    def check_row_counts():
        """Verify tables have data."""
        with DatabaseConnection.get_cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM raw.orders")
            count = cursor.fetchone()[0]
            logger.info(f"  raw.orders: {count:,} rows")
            return count > 0
    
    @staticmethod
    def check_nulls():
        """Check for unexpected NULL values."""
        with DatabaseConnection.get_cursor() as cursor:
            cursor.execute("""
                SELECT 
                    COUNT(*) FILTER (WHERE order_id IS NULL) as null_order_id,
                    COUNT(*) FILTER (WHERE customer_id IS NULL) as null_customer_id,
                    COUNT(*) FILTER (WHERE amount IS NULL) as null_amount,
                    COUNT(*) FILTER (WHERE order_timestamp IS NULL) as null_timestamp
                FROM raw.orders
            """)
            result = cursor.fetchone()
            logger.info(f"  NULL counts: order_id={result[0]}, customer_id={result[1]}, "
                       f"amount={result[2]}, timestamp={result[3]}")
            return sum(result) == 0
    
    @staticmethod
    def check_duplicates():
        """Check for duplicate order_ids."""
        with DatabaseConnection.get_cursor() as cursor:
            cursor.execute("""
                SELECT order_id, COUNT(*) as cnt
                FROM raw.orders
                GROUP BY order_id
                HAVING COUNT(*) > 1
                LIMIT 5
            """)
            duplicates = cursor.fetchall()
            if duplicates:
                logger.warning(f"  Found {len(duplicates)} duplicate order_ids:")
                for dup in duplicates:
                    logger.warning(f"    {dup[0]}: {dup[1]} occurrences")
                return False
            else:
                logger.info("  No duplicates found")
                return True
    
    @staticmethod
    def check_data_ranges():
        """Verify data is within expected ranges."""
        with DatabaseConnection.get_cursor() as cursor:
            cursor.execute("""
                SELECT 
                    MIN(amount) as min_amount,
                    MAX(amount) as max_amount,
                    MIN(order_timestamp) as min_date,
                    MAX(order_timestamp) as max_date
                FROM raw.orders
            """)
            result = cursor.fetchone()
            logger.info(f"  Amount range: ${result[0]:.2f} - ${result[1]:.2f}")
            logger.info(f"  Date range: {result[2]} to {result[3]}")
            
            # Check for negative amounts
            cursor.execute("SELECT COUNT(*) FROM raw.orders WHERE amount < 0")
            negative_count = cursor.fetchone()[0]
            if negative_count > 0:
                logger.warning(f"  Found {negative_count} negative amounts")
                return False
            
            return True
    
    @staticmethod
    def check_fraud_logic():
        """Verify fraud detection logic is correct."""
        with DatabaseConnection.get_cursor() as cursor:
            # Should be fraud: amount > 1000 AND customer_age_days < 30
            cursor.execute("""
                SELECT COUNT(*) 
                FROM raw.orders
                WHERE amount > 1000 
                  AND customer_age_days < 30 
                  AND is_fraud = FALSE
            """)
            false_negatives = cursor.fetchone()[0]
            
            # Should NOT be fraud
            cursor.execute("""
                SELECT COUNT(*) 
                FROM raw.orders
                WHERE (amount <= 1000 OR customer_age_days >= 30)
                  AND is_fraud = TRUE
            """)
            false_positives = cursor.fetchone()[0]
            
            logger.info(f"  False negatives (should be fraud): {false_negatives}")
            logger.info(f"  False positives (shouldn't be fraud): {false_positives}")
            
            return false_negatives == 0 and false_positives == 0
    
    @staticmethod
    def check_partitions():
        """Verify partition columns are populated."""
        with DatabaseConnection.get_cursor() as cursor:
            cursor.execute("""
                SELECT 
                    COUNT(DISTINCT year) as years,
                    COUNT(DISTINCT month) as months,
                    COUNT(DISTINCT day) as days
                FROM raw.orders
            """)
            result = cursor.fetchone()
            logger.info(f"  Partitions: {result[0]} years, {result[1]} months, {result[2]} days")
            return all(x > 0 for x in result)

if __name__ == "__main__":
    verifier = DataVerifier()
    verifier.run_all_checks()
    DatabaseConnection.close_pool()
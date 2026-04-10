@"
"""
Great Expectations data quality test for freshness
"""
import great_expectations as gx
from datetime import datetime, timedelta

def test_data_freshness():
    context = gx.get_context()
    
    # Define expectation
    suite = context.add_expectation_suite(
        expectation_suite_name="freshness_suite"
    )
    
    # Check that data is less than 1 hour old
    max_age_hours = 1
    threshold = datetime.now() - timedelta(hours=max_age_hours)
    
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="processed_time",
            min_value=threshold.isoformat(),
            max_value=datetime.now().isoformat()
        )
    )
    
    return suite
"@ | Out-File -FilePath "tests/great_expectations/freshness_check.py" -Encoding utf8
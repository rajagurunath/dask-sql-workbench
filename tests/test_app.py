import pytest
from app import main_function

# Example test case
@pytest.mark.parametrize(
    "input_data, expected_output",
    [
        ("test_input", "expected_output"),
        ("another_test_input", "another_expected_output")
    ]
)
def test_main_function(input_data, expected_output):
    result = main_function(input_data)
    assert result == expected_output


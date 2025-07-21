import pytest
from app import app


def test_app():
    assert app is not None
    # Added this line to address the comment
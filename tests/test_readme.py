import os


def test_readme_exists():
    assert os.path.exists('README.md'), "README.md does not exist"


def test_readme_contains_required_sections():
    required_sections = [
        "Project Overview",
        "Table of Contents",
        "Project Structure",
        "Dependencies",
        "Getting Started",
        "Usage",
        "Contributing"
    ]
    with open('README.md', 'r') as f:
        readme_content = f.read()
    for section in required_sections:
        assert section in readme_content, f"Section '{section}' is missing from README.md"


# Project Overview

This project utilizes Python along with several popular frameworks and libraries to build data-driven applications. Below is a detailed breakdown of the project structure, dependencies, and usage instructions.

## Table of Contents
- [Project Structure](#project-structure)
- [Dependencies](#dependencies)
- [Getting Started](#getting-started)
- [Usage](#usage)
- [Contributing](#contributing)

## Project Structure
The project is organized into the following directories and key files:

### Directories
- `src/`: Contains the main source code.
- `tests/`: Houses unit tests using Pytest.
- `data/`: Stores datasets or sample data used in the application.

### Key Files
- `requirements.txt`: Lists all production dependencies.
- `setup.py`: Configuration for packaging and distribution (if applicable).

## Dependencies
The project relies on the following dependencies:

### Production Dependencies
- `pandas_profiling==3.1.0`
- `streamlit==0.89.0`
- `coiled>=0.0.55`
- `streamlit_ace==0.1.0`
- `streamlit_pandas_profiling==0.1.2`
- `dask==2021.10.0`
- `numpy==1.21.2`
- `seaborn==0.11.2`
- `pandas==1.3.2`
- `matplotlib==3.4.3`
- `dask_sql==0.3.9`
- `xgboost`
- `dask_ml`
- `s3fs`
- `fsspec`
- `sklearn`
- `distributed`
- `textual`
- `rich`

### Development Dependencies
- `pytest`: Used for testing.

## Getting Started
1. **Clone the Repository**
   ```bash
   git clone https://github.com/yourusername/your-repo.git
   ```
2. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```
3. **Run the Application**
   ```bash
   streamlit run src/main.py
   ```

## Usage
The project leverages Streamlit for creating interactive web applications. It uses Dask for distributed computing and Pandas Profiling for exploratory data analysis. Detailed usage instructions for specific features or modules can be found in their respective directories.

## Contributing
1. Fork the repository and create a new branch for your feature or bug fix.
2. Ensure that your changes pass all tests before submitting a pull request.
3. Follow the existing coding style and conventions.

## License
MIT License


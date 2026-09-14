# GEO_E2E

GIS Data End-to-End Pipeline — a Python 3.11 application for programmatically downloading, processing, and managing geospatial data.

## Overview

GEO_E2E extends the GEO_Download project with an end-to-end pipeline for geospatial data management, including downloading, processing, and orchestration capabilities.

## Tech Stack

| Component        | Technology                                  |
|------------------|---------------------------------------------|
| Language         | Python 3.11                                 |
| Data Validation  | Pydantic >= 2.0.0                           |
| HTTP Client      | requests >= 2.31.0                          |
| Task Queue       | Celery >= 5.3.0                             |
| Timezone         | pytz >= 2023.3                              |
| Testing          | pytest >= 7.0.0                             |
| Environment      | Conda                                       |

## Project Structure

```
GEO_E2E/
├── app.py                # Main application entry point
├── config.json           # Data source configuration
├── util.py               # Utility functions
├── model.py              # Data models
├── environment.yml       # Conda environment definition
├── test/                 # Unit tests
│   ├── __init__.py
│   ├── test_models.py
│   └── test_util.py
├── data/                 # Downloaded data files
│   └── USGS_Earthquake/
└── logs/                 # Application logs
```

## Setup

```bash
conda env create -f environment.yml
conda activate geo_e2e_311
```

## Usage

```bash
python app.py
```

## Testing

```bash
pytest test/
```

## Related Projects

- **GEO_Download** - Base GIS data downloader with factory pattern and extensible architecture

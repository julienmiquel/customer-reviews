# Burger King Reviews in France 🍔

A Python tool that fetches and stores Burger King restaurant reviews in France using the Google Places API and Google BigQuery. This project helps analyze customer feedback and ratings across Burger King locations in France.

## Features

- 🔍 Search for Burger King locations in France using Google Places API
- ⭐ Fetch detailed reviews and ratings for each location
- 📊 Store data in Google BigQuery for analysis
- 🐳 Docker support for easy deployment
- 🧪 Comprehensive test suite
- 🔒 Secure API key management

## Prerequisites

- Python 3.9 or higher
- Poetry (Python package manager)
- Google Cloud Platform account
- Google Places API key
- Google BigQuery enabled project

## Installation

1. **Install Poetry** (if not already installed):
   ```bash
   curl -sSL https://install.python-poetry.org | python3 -
   ```

2. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd bk-maps
   ```

3. **Install dependencies**:
   ```bash
   poetry install
   ```

4. **Set up environment variables**:
   Create a `.env` file in the root directory:
   ```env
   GOOGLE_API_KEY=your_api_key
   GOOGLE_CLOUD_PROJECT=your_project_id
   ```

## Usage

### Local Development

1. **Activate the virtual environment**:
   ```bash
   poetry shell
   ```

2. **Run the main script**:
   ```bash
   poetry run python scripts/fetch_reviews.py
   ```

### Docker Deployment

1. **Build the Docker image**:
   ```bash
   docker build -t bk-maps .
   ```

2. **Run the container**:
   ```bash
   docker run -e GOOGLE_API_KEY=your_api_key -e GOOGLE_CLOUD_PROJECT=your_project_id bk-maps
   ```

## Development

### Code Quality Tools

- **Format code**:
  ```bash
  poetry run black .
  poetry run isort .
  ```

- **Type checking**:
  ```bash
  poetry run mypy .
  ```

- **Linting**:
  ```bash
  poetry run flake8
  ```

- **Run tests**:
  ```bash
  poetry run pytest
  ```

### Project Structure

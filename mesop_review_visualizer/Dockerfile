# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set environment variables for Python and Poetry
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=true \
    POETRY_VERSION=1.7.1 \
    POETRY_HOME="/opt/poetry" \
    POETRY_VIRTUALENVS_CREATE=false

# Add Poetry to PATH
ENV PATH="$POETRY_HOME/bin:$PATH"

# Set work directory
WORKDIR /app

# Install system dependencies (if any are needed beyond base Python for your app, e.g., for certain libraries)
# For now, keeping it minimal as per original slim image. Add gcc, python3-dev if needed by dependencies.
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

# Install Poetry
RUN curl -sSL https://install.python-poetry.org | python3 -

# Copy poetry configuration files from the project root
# These files define the dependencies for the whole project.
# The Docker build context for this Dockerfile should be the project root.
COPY pyproject.toml poetry.lock* ./

# Install dependencies using Poetry
# --no-root is used because we are not installing the root project as a package,
# but rather its dependencies to run the Mesop app.
# Consider adding --only main if you have dev dependencies you want to skip.
RUN poetry install --no-interaction --no-ansi --no-root

# Copy the Mesop application code into the container
# This assumes main.py is directly under mesop_review_visualizer/
COPY ./mesop_review_visualizer/ ./mesop_review_visualizer/

# Expose the port the Mesop app runs on
EXPOSE 3210

# Create a non-root user and switch to it (good practice)
RUN useradd -m appuser && chown -R appuser:appuser /app
USER appuser

# Command to run the Mesop application using Poetry
# This path is relative to the WORKDIR /app
CMD ["poetry", "run", "mesop", "mesop_review_visualizer/main.py"]

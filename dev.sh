export REVIEW_STRATEGY most_relevant

poetry run python scripts/fetch_reviews.py

export REVIEW_STRATEGY newest
poetry run python scripts/fetch_reviews.py
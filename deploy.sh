#gcloud run deploy reviews-update --source . --region europe-west9 --allow-unauthenticated 

PROJECT_ID=ml-demo-384110
REVIEW_STRATEGY="newest" # most_relevant

JOB_NAME=review-job-update
gcloud run jobs deploy $JOB_NAME \
    --source . \
    --tasks 1 \
    --set-env-vars GOOGLE_CLOUD_PROJECT=$PROJECT_ID \
    --set-env-vars REVIEW_STRATEGY=$REVIEW_STRATEGY \
    --max-retries 5 \
    --region europe-west9 \

gcloud run jobs execute $JOB_NAME --region europe-west9


#!/bin/bash -xe

printf $FTP_USER | gcloud secrets create FTP_USER --data-file=- \
    --replication-policy=user-managed \
    --locations=$GOOGLE_CLOUD_REGION;

printf $FTP_PASS | gcloud secrets create FTP_PASS --data-file=- \
    --replication-policy=user-managed \
    --locations=$GOOGLE_CLOUD_REGION;

printf $BQ_PROJECT_ID | gcloud secrets create BQ_PROJECT_ID --data-file=- \
    --replication-policy=user-managed \
    --locations=$GOOGLE_CLOUD_REGION;

printf $BQ_TABLE_STEM | gcloud secrets create BQ_TABLE_STEM --data-file=- \
    --replication-policy=user-managed \
    --locations=$GOOGLE_CLOUD_REGION;

printf $BQ_ACCOUNT_CREDS | gcloud secrets create BQ_ACCOUNT_CREDS --data-file=- \
    --replication-policy=user-managed \
    --locations=$GOOGLE_CLOUD_REGION;
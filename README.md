# Adobe GCS to Anthropic Translation Connector

This connector integrates Adobe's Globalization Content Service (GCS) with Anthropic's Claude AI for automated translation. It polls for translation tasks from Adobe GCS, processes them using the Anthropic API, and returns the translated content to Adobe.

## Features

- Polls the Adobe I/O Events Journaling API for translation tasks
- Handles both new translation tasks (TRANSLATE) and rejected tasks (RE_TRANSLATE)
- Uses Anthropic's Claude AI for translation
- Automatically refreshes OAuth tokens
- Comprehensive logging

## Prerequisites

- Adobe GCS account with API credentials
- Anthropic API key
- Python 3.7 or newer

## Setup

1. Clone this repository
2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```
3. Create a `.env` file with your credentials (see `.env.example`)
4. Run the connector:
   ```
   python adobe_gcs_connector.py
   ```

## Environment Variables

Create a `.env` file with the following variables:

```
# Adobe GCS credentials
ADOBE_CLIENT_ID=your_client_id
ADOBE_CLIENT_SECRET=your_client_secret
ADOBE_IMS_ORG_ID=your_org_id@AdobeOrg

# Journaling endpoint 
ADOBE_JOURNALING_ENDPOINT=organizations/xxxxx/integrations/xxxxx/xxxxx

# Anthropic API key
ANTHROPIC_API_KEY=your_anthropic_api_key

# Connector configuration
POLL_INTERVAL_SECONDS=30
```

## Hosting Options

There are several low-cost options for hosting this connector:

1. **Docker container on a VPS** (Digital Ocean, Linode, etc.)
2. **AWS Lambda** with a scheduled trigger (requires adapting the code)
3. **Google Cloud Run** with Cloud Scheduler
4. **Heroku** basic dyno
5. **Railway.app** starter plan
6. **Fly.io** free tier

See the Deployment Guide for instructions on each option.

## Docker Deployment

A Dockerfile is included for easy containerization:

```bash
# Build the image
docker build -t gcs-anthropic-connector .

# Run the container
docker run -d --name gcs-connector gcs-anthropic-connector
```

## Limitations

- The connector uses a simplified approach to translation that may not handle complex formatting
- Error handling could be improved for production use
- Rate limiting is not implemented for the Anthropic API

## License

MIT

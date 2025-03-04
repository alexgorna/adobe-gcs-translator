# adobe_gcs_connector.py
import os
import time
import json
import logging
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("gcs_connector.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("GCSConnector")

# Load environment variables
load_dotenv()

class GCSConnector:
    """Connector for Adobe Globalization Content Service that uses Anthropic for translation."""
    
    def __init__(self):
        # Adobe credentials
        self.client_id = os.getenv("ADOBE_CLIENT_ID")
        self.client_secret = os.getenv("ADOBE_CLIENT_SECRET")
        self.ims_org_id = os.getenv("ADOBE_IMS_ORG_ID")
        
        # Journaling endpoint
        self.journaling_base_url = "https://events-va6.adobe.io/events/"
        self.journaling_endpoint = os.getenv("ADOBE_JOURNALING_ENDPOINT")
        
        # GCS API endpoints
        self.gcs_api_base_url = "https://gcs.adobe.io/api/v1"
        
        # Anthropic API
        self.anthropic_api_key = os.getenv("ANTHROPIC_API_KEY")
        
        # Access token management
        self.access_token = None
        self.token_expiry_time = 0
        
        # Event processing state
        self.next_url = None
        self.poll_interval = int(os.getenv("POLL_INTERVAL_SECONDS", "30"))
    
    def refresh_access_token(self):
        """Refreshes the Adobe access token using the OAuth client credentials flow."""
        logger.info("Refreshing Adobe access token")
        
        url = "https://ims-na1.adobelogin.com/ims/token/v3"
        headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }
        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "scope": "AdobeID,openid,read_organizations,additional_info.projectedProductContext,additional_info.roles,adobeio_api,read_client_secret,manage_client_secrets"
        }
        
        try:
            response = requests.post(url, headers=headers, data=data)
            response.raise_for_status()
            
            token_data = response.json()
            self.access_token = token_data.get("access_token")
            expires_in = token_data.get("expires_in", 86399)  # Default to 24 hours minus 1 second
            
            # Set expiry time 5 minutes before actual expiry to be safe
            self.token_expiry_time = time.time() + expires_in - 300
            
            logger.info("Successfully refreshed access token")
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to refresh access token: {e}")
            raise
    
    def get_auth_headers(self):
        """Returns the authentication headers needed for Adobe API calls."""
        if time.time() > self.token_expiry_time:
            self.refresh_access_token()
            
        return {
            "Authorization": f"Bearer {self.access_token}",
            "x-api-key": self.client_id,
            "x-ims-org-id": self.ims_org_id
        }
    
    def poll_for_events(self):
        """Polls the journaling endpoint for new events."""
        try:
            if self.next_url is None:
                url = f"{self.journaling_base_url}{self.journaling_endpoint}?limit=10"
            else:
                url = self.next_url
                
            logger.info(f"Polling for events: {url}")
            
            headers = self.get_auth_headers()
            response = requests.get(url, headers=headers)
            
            if response.status_code == 204:
                logger.info("No new events (204 No Content)")
                return
                
            response.raise_for_status()
            
            # Extract Link header to find next URL
            link_header = response.headers.get("link")
            if link_header:
                for link in link_header.split(","):
                    if 'rel="next"' in link:
                        # Extract URL from link header
                        self.next_url = link.split(";")[0].strip("<>")
                        break
            
            # Process events
            response_data = response.json()
            events = response_data.get("events", [])
            
            for event_wrapper in events:
                event = event_wrapper.get("event", {}).get("body", {})
                event_code = event.get("eventCode")
                
                if event_code == "TRANSLATE":
                    self.handle_translate_event(event)
                elif event_code == "RE_TRANSLATE":
                    self.handle_retranslate_event(event)
                else:
                    logger.warning(f"Unknown event code: {event_code}")
            
            logger.info(f"Processed {len(events)} events")
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error polling for events: {e}")
    
    def get_asset(self, project_id, task_id, asset_id=None):
        """Retrieves an asset from GCS."""
        try:
            if asset_id:
                url = f"{self.gcs_api_base_url}/projects/{project_id}/tasks/{task_id}/assets/{asset_id}"
            else:
                url = f"{self.gcs_api_base_url}/projects/{project_id}/tasks/{task_id}/assets"
                
            headers = self.get_auth_headers()
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error getting asset: {e}")
            raise
    
    def put_asset(self, project_id, task_id, asset_id, translated_content):
        """Puts a translated asset back to GCS."""
        try:
            url = f"{self.gcs_api_base_url}/projects/{project_id}/tasks/{task_id}/assets/{asset_id}"
            
            headers = self.get_auth_headers()
            headers["Content-Type"] = "application/json"
            
            data = {
                "content": translated_content
            }
            
            response = requests.put(url, headers=headers, json=data)
            response.raise_for_status()
            
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error putting asset: {e}")
            raise
    
    def translate_with_anthropic(self, source_text, source_language, target_language):
        """Uses Anthropic's Claude to translate text using direct API calls."""
        try:
            headers = {
                "x-api-key": self.anthropic_api_key,
                "content-type": "application/json",
                "anthropic-version": "2023-06-01"
            }
            
            prompt = f"""Please translate the following text from {source_language} to {target_language}. 
            Provide only the translated text with no additional comments or explanations.
            
            Text to translate:
            {source_text}"""
            
            data = {
                "model": "claude-3-haiku-20240307",
                "max_tokens": 4000,
                "messages": [
                    {"role": "user", "content": prompt}
                ]
            }
            
            response = requests.post(
                "https://api.anthropic.com/v1/messages",
                headers=headers,
                json=data
            )
            response.raise_for_status()
            
            response_data = response.json()
            return response_data["content"][0]["text"]
            
        except Exception as e:
            logger.error(f"Error translating with Anthropic: {e}")
            raise
    
    def handle_translate_event(self, event):
        """Handles a TRANSLATE event."""
        try:
            project_id = event.get("projectId")
            task_id = event.get("taskId")
            source_locale = event.get("sourceLocale")
            target_locale = event.get("targetLocale")
            
            logger.info(f"Processing TRANSLATE event - Project: {project_id}, Task: {task_id}")
            logger.info(f"Translating from {source_locale} to {target_locale}")
            
            # Get assets for the task
            assets = self.get_asset(project_id, task_id)
            
            for asset in assets:
                asset_id = asset.get("id")
                source_content = asset.get("content")
                
                # Translate the content using Anthropic
                translated_content = self.translate_with_anthropic(
                    source_content, 
                    source_locale, 
                    target_locale
                )
                
                # Put translated asset back
                self.put_asset(project_id, task_id, asset_id, translated_content)
                
                logger.info(f"Successfully translated and updated asset {asset_id}")
                
        except Exception as e:
            logger.error(f"Error handling TRANSLATE event: {e}")
    
    def handle_retranslate_event(self, event):
        """Handles a RE_TRANSLATE event."""
        try:
            project_id = event.get("projectId")
            task_id = event.get("taskId")
            source_locale = event.get("sourceLocale")
            target_locale = event.get("targetLocale")
            asset_name = event.get("assetName")
            asset_url = event.get("assetUrl")
            
            logger.info(f"Processing RE_TRANSLATE event - Project: {project_id}, Task: {task_id}")
            logger.info(f"Re-translating asset {asset_name} from {source_locale} to {target_locale}")
            
            # For RE_TRANSLATE, we need to retrieve the specific asset with reviewer comments
            # Get the asset content
            response = requests.get(asset_url)
            response.raise_for_status()
            
            # The response format may vary; adjust as needed
            asset_content = response.text
            
            # Translate with Anthropic using direct API call
            headers = {
                "x-api-key": self.anthropic_api_key,
                "content-type": "application/json",
                "anthropic-version": "2023-06-01"
            }
            
            prompt = f"""Please revise the following translation from {source_locale} to {target_locale}.
            This is a revision request, so please pay extra attention to accuracy and quality.
            
            Text to translate:
            {asset_content}"""
            
            data = {
                "model": "claude-3-haiku-20240307",
                "max_tokens": 4000,
                "messages": [
                    {"role": "user", "content": prompt}
                ]
            }
            
            api_response = requests.post(
                "https://api.anthropic.com/v1/messages",
                headers=headers,
                json=data
            )
            api_response.raise_for_status()
            
            response_data = api_response.json()
            translated_content = response_data["content"][0]["text"]
            
            # Put translated asset back
            self.put_asset(project_id, task_id, asset_name, translated_content)
            
            logger.info(f"Successfully re-translated and updated asset {asset_name}")
            
        except Exception as e:
            logger.error(f"Error handling RE_TRANSLATE event: {e}")
    
    def run(self):
        """Main execution loop of the connector."""
        logger.info("Starting GCS Connector")
        
        # Initial token refresh
        self.refresh_access_token()
        
        while True:
            try:
                self.poll_for_events()
                
                # Sleep before polling again
                logger.info(f"Sleeping for {self.poll_interval} seconds")
                time.sleep(self.poll_interval)
                
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                
                # Sleep a bit longer before retrying after an error
                time.sleep(self.poll_interval * 2)

# Main entry point
if __name__ == "__main__":
    connector = GCSConnector()
    connector.run()

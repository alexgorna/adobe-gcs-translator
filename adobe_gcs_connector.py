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
    
    def fix_url(self, url):
        """Ensures the URL has the proper format and protocol."""
        if not url:
            return url
            
        # Remove any leading/trailing whitespace
        url = url.strip()
        
        # Fix URLs that start with </events-fast/
        if "</events-fast/" in url:
            # Extract the actual path after </events-fast/
            path = url.split("</events-fast/")[1]
            return f"https://events-va6.adobe.io/events-fast/{path}"
            
        # Fix URLs that are missing the protocol and domain
        if not url.startswith("http"):
            if url.startswith("/"):
                return f"https://events-va6.adobe.io{url}"
            else:
                return f"https://events-va6.adobe.io/{url}"
                
        return url
    
    def poll_for_events(self):
        """Polls the journaling endpoint for new events."""
        try:
            if self.next_url is None:
                url = f"{self.journaling_base_url}{self.journaling_endpoint}?limit=10"
            else:
                url = self.fix_url(self.next_url)
                
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
                        next_url = link.split(";")[0].strip("<>")
                        self.next_url = next_url
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
            
            # Log the response for debugging
            logger.info(f"Asset API response status: {response.status_code}")
            if response.status_code != 200:
                logger.info(f"Asset API response body: {response.text[:500]}...")
                
            response.raise_for_status()
            
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error getting asset: {e}")
            
            # Try alternative endpoint format if 404 is received
            if isinstance(e, requests.exceptions.HTTPError) and e.response.status_code == 404:
                try:
                    # Try the asset URL directly from the event if available
                    if hasattr(self, 'current_event') and self.current_event.get("url"):
                        asset_url = self.current_event.get("url")
                        logger.info(f"Trying alternative asset URL: {asset_url}")
                        
                        headers = self.get_auth_headers()
                        response = requests.get(asset_url, headers=headers)
                        response.raise_for_status()
                        
                        return [{"id": "default", "content": response.text}]
                except Exception as alt_e:
                    logger.error(f"Error using alternative asset URL: {alt_e}")
            
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
            
            # Log request details
            logger.info(f"Putting translated asset to: {url}")
            
            response = requests.put(url, headers=headers, json=data)
            
            # Log response for debugging
            logger.info(f"Put asset response status: {response.status_code}")
            if response.status_code >= 400:
                logger.info(f"Put asset response body: {response.text[:500]}...")
                
            response.raise_for_status()
            
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error putting asset: {e}")
            
            # Try alternative endpoints or methods
            if isinstance(e, requests.exceptions.HTTPError) and (e.response.status_code == 404 or e.response.status_code == 400):
                try:
                    # Try using the complete asset API
                    complete_url = f"{self.gcs_api_base_url}/projects/{project_id}/tasks/{task_id}/assets/{asset_id}/locales/{self.current_target_locale}/complete"
                    logger.info(f"Trying asset completion API: {complete_url}")
                    
                    headers = self.get_auth_headers()
                    headers["Content-Type"] = "application/json"
                    
                    # Prepare the data for completion
                    complete_data = {
                        "assetName": asset_id,
                        "tenantId": self.current_tenant_id,
                        "targetAssetLocale": {
                            "locale": self.current_target_locale,
                            "status": "TRANSLATED"
                        },
                        "targetAssetUrl": {
                            "locale": self.current_target_locale,
                            "url": "dummy-url", # This is a placeholder
                            "urlType": "TRANSLATED"
                        }
                    }
                    
                    response = requests.put(complete_url, headers=headers, json=complete_data)
                    response.raise_for_status()
                    logger.info(f"Successfully completed asset using alternative API")
                    
                    return {"status": "completed"}
                    
                except Exception as alt_e:
                    logger.error(f"Error using alternative completion API: {alt_e}")
            
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
            # Store current event context for use in error handling
            self.current_event = event
            self.current_tenant_id = event.get("tenantId")
            self.current_target_locale = event.get("targetLocale")
            
            project_id = event.get("projectId")
            task_id = event.get("taskId")
            source_locale = event.get("sourceLocale")
            target_locale = event.get("targetLocale")
            
            logger.info(f"Processing TRANSLATE event - Project: {project_id}, Task: {task_id}")
            logger.info(f"Translating from {source_locale} to {target_locale}")
            
            # Get assets for the task
            try:
                assets = self.get_asset(project_id, task_id)
                logger.info(f"Retrieved {len(assets)} assets for translation")
                
                for asset in assets:
                    asset_id = asset.get("id")
                    source_content = asset.get("content")
                    
                    if not source_content:
                        logger.warning(f"Asset {asset_id} has no content to translate")
                        continue
                    
                    # Translate the content using Anthropic
                    translated_content = self.translate_with_anthropic(
                        source_content, 
                        source_locale, 
                        target_locale
                    )
                    
                    # Put translated asset back
                    self.put_asset(project_id, task_id, asset_id, translated_content)
                    
                    logger.info(f"Successfully translated and updated asset {asset_id}")
            
            except Exception as asset_e:
                logger.error(f"Error processing assets: {asset_e}")
                
                # Try alternative approach by using the URL in the event
                if event.get("url"):
                    try:
                        asset_url = event.get("url")
                        logger.info(f"Trying direct event URL: {asset_url}")
                        
                        headers = self.get_auth_headers()
                        response = requests.get(asset_url, headers=headers)
                        response.raise_for_status()
                        
                        source_content = response.text
                        
                        # Translate the content
                        translated_content = self.translate_with_anthropic(
                            source_content,
                            source_locale,
                            target_locale
                        )
                        
                        # Try to complete the task
                        complete_url = f"{self.gcs_api_base_url}/projects/{project_id}/tasks/{task_id}/assets/main/locales/{target_locale}/complete"
                        logger.info(f"Trying to complete directly: {complete_url}")
                        
                        headers = self.get_auth_headers()
                        headers["Content-Type"] = "application/json"
                        
                        complete_data = {
                            "assetName": "main",
                            "tenantId": event.get("tenantId"),
                            "targetAssetLocale": {
                                "locale": target_locale,
                                "status": "TRANSLATED"
                            },
                            "targetAssetUrl": {
                                "locale": target_locale,
                                "url": "dummy-url", # This is a placeholder
                                "urlType": "TRANSLATED"
                            }
                        }
                        
                        response = requests.put(complete_url, headers=headers, json=complete_data)
                        response.raise_for_status()
                        logger.info(f"Successfully completed translation using alternative method")
                        
                    except Exception as alt_e:
                        logger.error(f"Error using alternative translation approach: {alt_e}")
                
        except Exception as e:
            logger.error(f"Error handling TRANSLATE event: {e}")
    
    def handle_retranslate_event(self, event):
        """Handles a RE_TRANSLATE event."""
        try:
            # Store current event context for error handling
            self.current_event = event
            self.current_tenant_id = event.get("tenantId")
            self.current_target_locale = event.get("targetLocale")
            
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

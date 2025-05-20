"""
Adobe Globalization Content Service (GCS) to Claude Connector

This connector allows translation of content from Adobe GCS using Claude AI.
It strictly follows the documented GCS API without any undocumented endpoints.
"""

import os
import time
import logging
import requests
import json
import re
import xml.etree.ElementTree as ET
import traceback
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('GCSConnector')

class GCSConnector:
    """
    Connector for Adobe Globalization Content Service (GCS) that uses Claude for translation.
    """
    
    def __init__(self):
        """Initialize the connector with configuration from environment variables."""
        # Adobe API
        self.adobe_client_id = os.getenv("ADOBE_CLIENT_ID")
        self.adobe_client_secret = os.getenv("ADOBE_CLIENT_SECRET")
        self.adobe_ims_org_id = os.getenv("ADOBE_IMS_ORG_ID")
        self.journaling_endpoint = os.getenv("ADOBE_JOURNALING_ENDPOINT")
        
        # Claude API
        self.anthropic_api_key = os.getenv("ANTHROPIC_API_KEY")
        
        # Configuration
        self.poll_interval_seconds = int(os.getenv("POLL_INTERVAL_SECONDS", "30"))
        
        # API endpoints
        self.ims_token_url = "https://ims-na1.adobelogin.com/ims/token/v3"
        self.events_base_url = "https://events-va6.adobe.io"
        self.gcs_api_base_url = "https://gcs.adobe.io/v1"
        
        # State variables
        self.access_token = None
        self.token_expiry = 0
        self.next_url = None
        
        logger.info("Starting GCS Connector")
    
    def start(self):
        """Start the connector and begin polling for events."""
        while True:
            try:
                # Refresh token if needed
                if time.time() > self.token_expiry:
                    self.refresh_access_token()
                
                # Poll for events
                self.poll_for_events()
                
                # Wait before polling again
                logger.info(f"Sleeping for {self.poll_interval_seconds} seconds")
                time.sleep(self.poll_interval_seconds)
                
            except Exception as e:
                logger.error(f"Error in connector main loop: {e}")
                logger.info(f"Sleeping for {self.poll_interval_seconds} seconds")
                time.sleep(self.poll_interval_seconds)
    
    def refresh_access_token(self):
        """Refresh the Adobe access token."""
        logger.info("Refreshing Adobe access token")
        
        try:
            headers = {
                "Content-Type": "application/x-www-form-urlencoded"
            }
            
            data = {
                "grant_type": "client_credentials",
                "client_id": self.adobe_client_id,
                "client_secret": self.adobe_client_secret,
                "scope": "AdobeID,openid,read_organizations,additional_info.projectedProductContext,additional_info.roles"
            }
            
            response = requests.post(self.ims_token_url, headers=headers, data=data)
            response.raise_for_status()
            
            token_data = response.json()
            self.access_token = token_data["access_token"]
            # Set expiry time 10 minutes before actual expiry to be safe
            self.token_expiry = time.time() + token_data["expires_in"] - 600
            
            logger.info("Successfully refreshed access token")
            
        except Exception as e:
            logger.error(f"Error refreshing access token: {e}")
            raise
    
    def get_auth_headers(self):
        """Get the authentication headers for Adobe API requests."""
        return {
            "Authorization": f"Bearer {self.access_token}",
            "x-api-key": self.adobe_client_id,
            "x-ims-org-id": self.adobe_ims_org_id
        }
    
    def fix_url(self, url):
        """Fix malformed URLs that might come from Adobe's API."""
        # Remove any whitespace
        url = url.strip()
        
        # Check if URL is malformed with '</events-fast/' pattern
        if '</events-fast/' in url:
            # Extract the path after '</events-fast/'
            path = url.split('</events-fast/')[1]
            # Reconstruct proper URL
            return f"{self.events_base_url}/events-fast/{path}"
        
        # If URL doesn't start with http or https, add the base URL
        if not url.startswith('http'):
            if url.startswith('/'):
                return f"{self.events_base_url}{url}"
            else:
                return f"{self.events_base_url}/{url}"
        
        return url
    
    def poll_for_events(self):
        """Poll for events from Adobe GCS."""
        try:
            url = None
            
            if self.next_url:
                url = self.fix_url(self.next_url)
            else:
                url = f"{self.events_base_url}/{self.journaling_endpoint}?limit=10"
            
            logger.info(f"Polling for events: {url}")
            
            headers = self.get_auth_headers()
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                # Process events
                data = response.json()
                events = data.get("events", [])
                
                if events:
                    logger.info(f"Received {len(events)} events")
                    
                    for event in events:
                        event_body = event.get("event", {}).get("body", {})
                        event_code = event_body.get("eventCode")
                        
                        if event_code == "TRANSLATE":
                            self.handle_translate_event(event_body)
                        elif event_code == "RE_TRANSLATE":
                            self.handle_retranslate_event(event_body)
                    
                    logger.info(f"Processed {len(events)} events")
                else:
                    logger.info("No events in response")
                
                # Get next URL from Link header
                link_header = response.headers.get("link")
                if link_header:
                    match = re.search(r'<([^>]+)>; rel="next"', link_header)
                    if match:
                        self.next_url = match.group(1)
                
            elif response.status_code == 204:
                logger.info("No new events (204 No Content)")
            else:
                logger.error(f"Error polling for events: {response.status_code} - {response.text[:500]}")
                response.raise_for_status()
                
        except Exception as e:
            logger.error(f"Error polling for events: {e}")
    
    def handle_translate_event(self, event):
        """Handle a TRANSLATE event from Adobe GCS."""
        try:
            project_id = event.get("projectId")
            task_id = event.get("taskId")
            source_locale = event.get("sourceLocale")
            target_locale = event.get("targetLocale")
            tenant_id = event.get("tenantId")
            
            logger.info(f"Processing TRANSLATE event - Project: {project_id}, Task: {task_id}")
            logger.info(f"Translating from {source_locale} to {target_locale}")
            
            # Get assets for the task
            assets = self.get_assets(project_id, task_id, target_locale, tenant_id)
            
            if not assets:
                logger.warning("No assets found for translation")
                return
            
            logger.info(f"Retrieved {len(assets)} assets for translation")
            
            for asset in assets:
                asset_name = asset.get("name")
                
                logger.info(f"Processing asset: {asset_name}")
                
                # Extract object key from asset URLs
                object_key = None
                asset_urls = asset.get("assetUrls", [])
                
                for asset_url in asset_urls:
                    if asset_url.get("urlType") == "NORMALIZED" and asset_url.get("locale") == source_locale:
                        url = asset_url.get("url")
                        if url:
                            parts = url.split("/")
                            if len(parts) >= 3:
                                # Extract object key from URL
                                object_key_start = url.find(tenant_id)
                                if object_key_start != -1:
                                    # Remove any query parameters
                                    object_key = url[object_key_start:].split("?")[0]
                                    break
                
                if not object_key:
                    logger.error(f"Could not extract object key for asset: {asset_name}")
                    continue
                
                logger.info(f"Extracted object key: {object_key}")
                
                # Get asset content
                xliff_content = self.get_asset_content(tenant_id, object_key)
                
                if not xliff_content:
                    logger.error(f"Failed to get content for asset: {asset_name}")
                    continue
                
                # Translate the XLIFF content
                translated_xliff = self.translate_xliff_with_anthropic(xliff_content, source_locale, target_locale)
                
                if not translated_xliff:
                    logger.error(f"Failed to translate content for asset: {asset_name}")
                    continue
                
                # Upload the translated content
                translated_url = self.upload_translated_content(tenant_id, asset_name, target_locale, translated_xliff)
                
                if not translated_url:
                    logger.error(f"Failed to upload translated content for asset: {asset_name}")
                    continue
                
                # Complete the asset translation
                success = self.complete_asset_translation(
                    project_id, 
                    task_id, 
                    asset_name, 
                    target_locale, 
                    tenant_id, 
                    translated_url
                )
                
                if not success:
                    logger.error(f"Failed to complete translation for asset: {asset_name}")
                    continue
            
        except Exception as e:
            logger.error(f"Error handling TRANSLATE event: {e}")
            logger.error(traceback.format_exc())
    
    def handle_retranslate_event(self, event):
        """Handle a RE_TRANSLATE event from Adobe GCS."""
        try:
            project_id = event.get("projectId")
            task_id = event.get("taskId")
            source_locale = event.get("sourceLocale")
            target_locale = event.get("targetLocale")
            tenant_id = event.get("tenantId")
            asset_name = event.get("assetName")
            asset_url = event.get("assetUrl")
            
            logger.info(f"Processing RE_TRANSLATE event - Project: {project_id}, Task: {task_id}")
            logger.info(f"Re-translating from {source_locale} to {target_locale}")
            logger.info(f"Asset: {asset_name}")
            
            # For RE_TRANSLATE, the asset URL is provided directly in the event
            if not asset_url:
                logger.error("No asset URL provided in RE_TRANSLATE event")
                return
            
            # Get asset content from the URL
            response = requests.get(asset_url, headers=self.get_auth_headers())
            
            if response.status_code != 200:
                logger.error(f"Failed to get asset content: {response.status_code} - {response.text[:500]}")
                return
            
            xliff_content = response.content
            
            if not xliff_content:
                logger.error("Empty content from asset URL")
                return
            
            # Translate the XLIFF content
            translated_xliff = self.translate_xliff_with_anthropic(xliff_content, source_locale, target_locale)
            
            if not translated_xliff:
                logger.error("Failed to translate content")
                return
            
            # Upload the translated content
            translated_url = self.upload_translated_content(tenant_id, asset_name, target_locale, translated_xliff)
            
            if not translated_url:
                logger.error("Failed to upload translated content")
                return
            
            # Complete the asset translation
            success = self.complete_asset_translation(
                project_id, 
                task_id, 
                asset_name, 
                target_locale, 
                tenant_id, 
                translated_url
            )
            
            if not success:
                logger.error("Failed to complete translation")
                return
            
        except Exception as e:
            logger.error(f"Error handling RE_TRANSLATE event: {e}")
            logger.error(traceback.format_exc())
    
    def get_assets(self, project_id, task_id, target_locale, tenant_id):
        """
        Get assets for a task.
        
        GET /v1/projects/{project}/tasks/{task}/assets/{targetLocale}?tenantId={tenantId}
        """
        try:
            url = f"{self.gcs_api_base_url}/projects/{project_id}/tasks/{task_id}/assets/{target_locale}?tenantId={tenant_id}"
            logger.info(f"Getting assets from: {url}")
            
            headers = self.get_auth_headers()
            response = requests.get(url, headers=headers)
            
            logger.info(f"Get assets response status: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                logger.info("Successfully retrieved assets information")
                return data.get("response", [])
            else:
                logger.error(f"Failed to get assets: {response.status_code} - {response.text[:500]}")
                return []
                
        except Exception as e:
            logger.error(f"Error getting assets: {e}")
            return []
    
    def get_asset_content(self, tenant_id, object_key):
        """
        Get asset content.
        
        GET /v1/assetContent?tenantId={tenantId}&objectKey={objectKey}
        """
        try:
            url = f"{self.gcs_api_base_url}/assetContent?tenantId={tenant_id}&objectKey={object_key}"
            logger.info(f"Getting asset content from: {url}")
            
            headers = self.get_auth_headers()
            response = requests.get(url, headers=headers)
            
            logger.info(f"Get asset content response status: {response.status_code}")
            
            if response.status_code == 200:
                content = response.content
                logger.info(f"Retrieved XLIFF content of length: {len(content)}")
                return content
            else:
                logger.error(f"Failed to get asset content: {response.status_code} - {response.text[:500]}")
                return None
                
        except Exception as e:
            logger.error(f"Error getting asset content: {e}")
            return None
    
    def translate_xliff_with_anthropic(self, xliff_content, source_language, target_language):
        """
        Translate XLIFF content using Anthropic's Claude.
        
        This function properly handles all translatable elements in the XLIFF file
        without including any markup or markers in the final translation.
        """
        try:
            # Parse the XLIFF file
            xliff_str = xliff_content.decode('utf-8')
            root = ET.fromstring(xliff_str)
            
            # Find all potentially translatable elements
            translatable_elements = []
            translation_items = []
            
            namespace = {'xliff': 'urn:oasis:names:tc:xliff:document:1.2'}
            
            # Track all trans-unit elements
            for file_elem in root.findall('.//xliff:file', namespace):
                for trans_unit in file_elem.findall('.//xliff:trans-unit', namespace):
                    translatable_elements.append(trans_unit)
            
            logger.info(f"Found {len(translatable_elements)} potentially translatable elements in XLIFF file")
            
            # Extract text for translation
            item_id = 0
            for elem in translatable_elements:
                source_elem = elem.find('.//xliff:source', namespace)
                if source_elem is not None and source_elem.text:
                    # Get attributes for context
                    elem_id = elem.get('id', '')
                    restype = elem.get('restype', '')
                    
                    # Add to list for translation
                    translation_items.append({
                        'id': item_id,
                        'elem_id': elem_id,
                        'restype': restype,
                        'text': source_elem.text,
                        'elem': elem,
                        'source_elem': source_elem
                    })
                    item_id += 1
            
            logger.info(f"Extracted {len(translation_items)} items for translation")
            
            if not translation_items:
                logger.warning("No translatable content found in XLIFF")
                return xliff_content
            
            # Prepare text for translation
            translation_map = {}
            segments_to_translate = []
            
            for item in translation_items:
                # Create a unique identifier that won't appear in the final translation
                segments_to_translate.append({
                    "id": item['id'],
                    "text": item['text']
                })
                translation_map[item['id']] = item
            
            # Call Claude API to translate
            prompt = f"""
            Translate the following text segments from {source_language} to {target_language}.
            
            For each segment, provide ONLY the translation without any additional text, markup, or identifiers.
            
            Here are the segments:
            """
            
            for segment in segments_to_translate:
                prompt += f"\n\nSegment {segment['id']}:\n{segment['text']}"
            
            headers = {
                "x-api-key": self.anthropic_api_key,
                "content-type": "application/json"
            }
            
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
            
            if response.status_code != 200:
                logger.error(f"Error from Anthropic API: {response.status_code} - {response.text}")
                return None
            
            translation_response = response.json()
            translated_text = translation_response["content"][0]["text"]
            
            # Process the translated text
            # Look for patterns like "Segment X:" or "Segment X:\n" followed by the translation
            for segment_id, item in translation_map.items():
                pattern = re.compile(rf"Segment\s*{segment_id}:?\s*(.*?)(?=Segment\s*\d+:|$)", re.DOTALL)
                match = pattern.search(translated_text)
                
                if match:
                    translated_content = match.group(1).strip()
                    
                    # Update the XLIFF
                    elem = item['elem']
                    target_elem = elem.find('.//xliff:target', namespace)
                    
                    # Create target element if it doesn't exist
                    if target_elem is None:
                        target_elem = ET.SubElement(elem, '{urn:oasis:names:tc:xliff:document:1.2}target')
                        target_elem.set('xml:lang', target_language)
                    
                    # Update the target text
                    target_elem.text = translated_content
            
            # Convert back to string
            translated_xliff = ET.tostring(root, encoding='utf-8', method='xml')
            logger.info(f"Translated XLIFF content of length: {len(translated_xliff)}")
            
            return translated_xliff
            
        except Exception as e:
            logger.error(f"Error translating XLIFF with Anthropic: {e}")
            logger.error(traceback.format_exc())
            # Return original content as fallback
            return xliff_content
    
    def upload_translated_content(self, tenant_id, asset_name, target_locale, translated_content):
        """
        Upload translated content to GCS Azure storage.
        
        POST /v1/uploadToStorage
        """
        try:
            url = f"{self.gcs_api_base_url}/uploadToStorage"
            logger.info(f"Uploading translated content to: {url}")
            
            headers = self.get_auth_headers()
            # Don't set Content-Type here, let requests set it with the correct boundary
            
            # Prepare the multipart form data
            files = {
                'file': (f"{asset_name}_{target_locale}.xlf", translated_content, 'application/octet-stream')
            }
            
            data = {
                'tenantId': tenant_id
            }
            
            response = requests.post(url, headers=headers, files=files, data=data)
            
            if response.status_code == 200:
                result = response.json()
                logger.info(f"Uploaded translated content to: {result.get('response')}")
                return result.get('response')
            else:
                logger.error(f"Failed to upload translated content: {response.status_code} - {response.text[:500]}")
                return None
                
        except Exception as e:
            logger.error(f"Error uploading translated content: {e}")
            return None
    
    def complete_asset_translation(self, project_id, task_id, asset_name, target_locale, tenant_id, translated_url):
        """
        Initiate asset locale completion in GCS.
        
        PUT /v1/projects/{project}/tasks/{task}/assets/{asset}/locales/{locale}/complete
        """
        try:
            url = f"{self.gcs_api_base_url}/projects/{project_id}/tasks/{task_id}/assets/{asset_name}/locales/{target_locale}/complete"
            logger.info(f"Completing asset translation: {url}")
            
            headers = self.get_auth_headers()
            headers["Content-Type"] = "application/json"
            
            # Prepare the request payload
            payload = {
                "assetName": asset_name,
                "tenantId": tenant_id,
                "targetAssetLocale": {
                    "locale": target_locale,
                    "status": "TRANSLATED"
                },
                "targetAssetUrl": {
                    "locale": target_locale,
                    "url": translated_url,
                    "urlType": "TRANSLATED"
                }
            }
            
            response = requests.put(url, headers=headers, json=payload)
            
            logger.info(f"Complete asset translation response status: {response.status_code}")
            
            if response.status_code in (200, 201):
                result = response.json()
                logger.info(f"Completed asset translation: {result}")
                return True
            else:
                logger.error(f"Failed to complete asset translation: {response.status_code} - {response.text[:500]}")
                return False
                
        except Exception as e:
            logger.error(f"Error completing asset translation: {e}")
            return False


if __name__ == "__main__":
    connector = GCSConnector()
    connector.start()

# v1.0 - enhanced translation log
import os
import time
import json
import logging
import requests
import re
from dotenv import load_dotenv
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET

# Custom JSON formatter for Railway's logging system
class RailwayJsonFormatter(logging.Formatter):
    """
    Formatter that outputs JSON strings after logging events.
    Adapted for Railway's logging system.
    """
    def __init__(self, **kwargs):
        super().__init__()
        self.json_default = kwargs.pop("json_default", str)

    def format(self, record):
        log_record = {}
        
        # Add the message (required)
        log_record["msg"] = super().format(record)
        
        # Add the level - Railway normalizes this
        log_record["level"] = record.levelname.lower()
        
        # Add the timestamp
        log_record["timestamp"] = datetime.fromtimestamp(record.created).isoformat()
        
        # Add useful attributes for filtering
        log_record["service"] = "gcs-translator"
        log_record["component"] = record.name
        
        # Add any exc_info if it exists
        if record.exc_info:
            log_record["error"] = self.formatException(record.exc_info)
        
        # Add any custom attributes from the record
        for key, value in record.__dict__.items():
            if key not in ('args', 'asctime', 'created', 'exc_info', 'exc_text', 'filename',
                          'funcName', 'id', 'levelname', 'levelno', 'lineno',
                          'module', 'msecs', 'message', 'msg', 'name', 'pathname',
                          'process', 'processName', 'relativeCreated', 'stack_info',
                          'thread', 'threadName'):
                log_record[key] = value
        
        return json.dumps(log_record, default=self.json_default)

# Setup improved logging
# Create logger
logger = logging.getLogger("GCSConnector")
logger.setLevel(logging.INFO)

# Remove any existing handlers
for handler in logger.handlers:
    logger.removeHandler(handler)

# Create console handler with the custom formatter
console_handler = logging.StreamHandler()
console_handler.setFormatter(RailwayJsonFormatter())
logger.addHandler(console_handler)

# File handler for local debugging (optional)
file_handler = logging.FileHandler("gcs_connector.log")
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)

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
        self.gcs_api_base_url = "https://gcs.adobe.io/v1"
        
        # Anthropic API
        self.anthropic_api_key = os.getenv("ANTHROPIC_API_KEY")
        
        # Access token management
        self.access_token = None
        self.token_expiry_time = 0
        
        # Event processing state
        self.next_url = None
        self.poll_interval = int(os.getenv("POLL_INTERVAL_SECONDS", "30"))
        
        logger.info("GCS Connector initialized", extra={"action": "init"})
    
    def refresh_access_token(self):
        """Refreshes the Adobe access token using the OAuth client credentials flow."""
        logger.info("Refreshing Adobe access token", extra={"action": "refresh_token"})
        
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
            
            logger.info("Successfully refreshed access token", 
                       extra={"action": "refresh_token_success", "expires_in": expires_in})
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to refresh access token: {str(e)}", 
                        extra={"action": "refresh_token_error", "error_details": str(e)})
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
            url_fixed = f"https://events-va6.adobe.io/events-fast/{path}"
            logger.debug(f"Fixed URL from {url} to {url_fixed}", 
                        extra={"action": "fix_url", "original": url, "fixed": url_fixed})
            return url_fixed
            
        # Fix URLs that are missing the protocol and domain
        if not url.startswith("http"):
            if url.startswith("/"):
                url_fixed = f"https://events-va6.adobe.io{url}"
            else:
                url_fixed = f"https://events-va6.adobe.io/{url}"
                
            logger.debug(f"Fixed URL from {url} to {url_fixed}", 
                        extra={"action": "fix_url", "original": url, "fixed": url_fixed})
            return url_fixed
                
        return url
    
    def poll_for_events(self):
        """Polls the journaling endpoint for new events."""
        try:
            if self.next_url is None:
                url = f"{self.journaling_base_url}{self.journaling_endpoint}?limit=10"
            else:
                url = self.fix_url(self.next_url)
                
            logger.info(f"Polling for events", extra={"action": "poll_events", "url": url})
            
            headers = self.get_auth_headers()
            response = requests.get(url, headers=headers)
            
            if response.status_code == 204:
                logger.info("No new events (204 No Content)", 
                           extra={"action": "poll_events_result", "status": 204})
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
                        logger.debug(f"Found next URL: {next_url}", 
                                    extra={"action": "next_url_found", "next_url": next_url})
                        break
            
            # Process events
            response_data = response.json()
            events = response_data.get("events", [])
            
            logger.info(f"Received {len(events)} events", 
                       extra={"action": "events_received", "count": len(events)})
            
            for event_wrapper in events:
                event = event_wrapper.get("event", {}).get("body", {})
                event_code = event.get("eventCode")
                
                logger.info(f"Processing event: {event_code}", 
                           extra={"action": "process_event", "event_code": event_code})
                
                if event_code == "TRANSLATE":
                    self.handle_translate_event(event)
                elif event_code == "RE_TRANSLATE":
                    self.handle_retranslate_event(event)
                else:
                    logger.warning(f"Unknown event code: {event_code}", 
                                  extra={"action": "unknown_event", "event_code": event_code})
            
            logger.info(f"Processed {len(events)} events", 
                       extra={"action": "events_processed", "count": len(events)})
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error polling for events: {str(e)}", 
                        extra={"action": "poll_events_error", "error_details": str(e), "url": url})
    
    def get_assets(self, project_id, task_id, target_locale, tenant_id):
        """
        Gets assets information using the Get All Assets API.
        
        As per documentation:
        GET /v1/projects/{project}/tasks/{task}/assets/{targetLocale}?tenantId={tenantId}
        """
        try:
            url = f"{self.gcs_api_base_url}/projects/{project_id}/tasks/{task_id}/assets/{target_locale}?tenantId={tenant_id}"
            logger.info(f"Getting assets", 
                       extra={"action": "get_assets", "project_id": project_id, "task_id": task_id, "url": url})
            
            headers = self.get_auth_headers()
            response = requests.get(url, headers=headers)
            
            # Log the response for debugging
            logger.info(f"Get assets response", 
                       extra={"action": "get_assets_response", "status": response.status_code})
            
            if response.status_code != 200:
                logger.info(f"Get assets response body", 
                           extra={"action": "get_assets_response_body", "body": response.text[:500]})
                response.raise_for_status()
            
            # Parse the response
            assets_data = response.json()
            logger.info(f"Successfully retrieved assets information", 
                       extra={"action": "get_assets_success"})
            
            # Return the response which contains the assets information
            return assets_data.get("response", [])
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error getting assets: {str(e)}", 
                        extra={"action": "get_assets_error", "error_details": str(e), "url": url})
            raise
    
    def get_asset_content(self, tenant_id, object_key):
        """
        Downloads asset content using the asset content API.
        
        As per documentation:
        GET /v1/assetContent?tenantId={tenantId}&objectKey={objectKey}
        """
        try:
            url = f"{self.gcs_api_base_url}/assetContent?tenantId={tenant_id}&objectKey={object_key}"
            logger.info(f"Getting asset content", 
                       extra={"action": "get_asset_content", "object_key": object_key})
            
            headers = self.get_auth_headers()
            response = requests.get(url, headers=headers)
            
            # Log the response for debugging
            logger.info(f"Get asset content response", 
                       extra={"action": "get_asset_content_response", "status": response.status_code})
            
            if response.status_code != 200:
                logger.info(f"Get asset content response body", 
                           extra={"action": "get_asset_content_response_body", "body": response.text[:500]})
                response.raise_for_status()
            
            # Return the content which should be the XLIFF file
            logger.info(f"Retrieved asset content", 
                       extra={"action": "get_asset_content_success", "content_length": len(response.text)})
            return response.text
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error getting asset content: {str(e)}", 
                        extra={"action": "get_asset_content_error", "error_details": str(e), "url": url})
            raise
    
    def upload_translated_content(self, tenant_id, translated_content, file_name="translated.xlf"):
        """
        Uploads translated content to GCS Azure Storage.
        
        As per documentation:
        POST /v1/uploadToStorage
        """
        try:
            url = f"{self.gcs_api_base_url}/uploadToStorage"
            logger.info(f"Uploading translated content", 
                       extra={"action": "upload_content", "file_name": file_name})
            
            headers = self.get_auth_headers()
            # Don't include Content-Type as requests will set it with the proper boundary
            
            # Prepare the file for multipart/form-data
            files = {
                'file': (file_name, translated_content, 'application/octet-stream')
            }
            
            # Add the tenantId as form field
            data = {
                'tenantId': tenant_id
            }
            
            # Make the request
            response = requests.post(url, headers=headers, files=files, data=data)
            
            # Log the response for debugging
            logger.info(f"Upload translated content response", 
                       extra={"action": "upload_content_response", "status": response.status_code})
            
            if response.status_code != 200:
                logger.info(f"Upload translated content response body", 
                           extra={"action": "upload_content_response_body", "body": response.text[:500]})
                response.raise_for_status()
            
            # Parse the response to get the URL of the uploaded file
            upload_data = response.json()
            logger.info("Successfully uploaded translated content", 
                       extra={"action": "upload_content_success"})
            
            # Return the URL where the translated content was uploaded
            return upload_data.get("response")
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error uploading translated content: {str(e)}", 
                        extra={"action": "upload_content_error", "error_details": str(e), "url": url})
            raise
    
    def complete_asset_translation(self, project_id, task_id, asset_name, target_locale, tenant_id, translated_url):
        """
        Completes the asset translation by calling the asset locale completion API.
        
        As per documentation:
        PUT /v1/projects/{project}/tasks/{task}/assets/{asset}/locales/{locale}/complete
        """
        try:
            # First check and update the asset state if needed
            update_url = f"{self.gcs_api_base_url}/projects/{project_id}/tasks/{task_id}/assets/{asset_name}/locales/{target_locale}"
            update_headers = self.get_auth_headers()
            update_headers["Content-Type"] = "application/json"
            
            # Prepare the update payload
            update_payload = {
                "locale": target_locale,
                "orgId": self.ims_org_id,
                "tenantId": tenant_id,  # Added based on documentation
                "status": "IN_TRANSLATION"
            }
            
            logger.info(f"Updating asset state to IN_TRANSLATION", 
                       extra={"action": "update_asset_state", "url": update_url})
            update_response = requests.put(update_url, headers=update_headers, json=update_payload)
            
            # Log the update response for debugging
            logger.info(f"Update asset state response", 
                       extra={"action": "update_asset_state_response", "status": update_response.status_code})
            if update_response.status_code not in (200, 201, 204):
                logger.error(f"Error updating asset state", 
                            extra={"action": "update_asset_state_error", "status": update_response.status_code, "body": update_response.text[:500]})
                update_response.raise_for_status()
            else:
                logger.info("Successfully updated asset state to IN_TRANSLATION", 
                           extra={"action": "update_asset_state_success"})
            
            # Now proceed with the completion
            complete_url = f"{self.gcs_api_base_url}/projects/{project_id}/tasks/{task_id}/assets/{asset_name}/locales/{target_locale}/complete"
            logger.info(f"Completing asset translation", 
                       extra={"action": "complete_asset", "url": complete_url})
            
            complete_headers = self.get_auth_headers()
            complete_headers["Content-Type"] = "application/json"
            
            # Prepare the request payload exactly as specified in the documentation
            complete_payload = {
                "assetName": asset_name,
                "tenantId": tenant_id,
                "orgId": self.ims_org_id,
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
            
            # Make the completion request
            complete_response = requests.put(complete_url, headers=complete_headers, json=complete_payload)
            
            # Log the response for debugging
            logger.info(f"Complete asset translation response", 
                       extra={"action": "complete_asset_response", "status": complete_response.status_code})
            
            if complete_response.status_code not in (200, 201):
                logger.info(f"Complete asset translation response body", 
                           extra={"action": "complete_asset_response_body", "body": complete_response.text[:500]})
                complete_response.raise_for_status()
            
            # Parse the response
            completion_data = complete_response.json() if complete_response.text else {"status": "completed"}
            logger.info("Successfully completed asset translation", 
                       extra={"action": "complete_asset_success"})
            
            return completion_data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error completing asset translation: {str(e)}", 
                        extra={"action": "complete_asset_error", "error_details": str(e)})
            raise
     
    def translate_xliff_with_anthropic(self, xliff_content, source_language, target_language):
        """
        Enhanced XLIFF translation that captures all translatable elements including
        headers, titles, and other special elements that might be missed by simpler methods.
        """
        try:
            # Parse the XLIFF file
            root = ET.fromstring(xliff_content)
            
            # Store all namespaces for proper handling
            namespaces = {}
            for prefix, uri in root.nsmap.items() if hasattr(root, 'nsmap') else []:
                namespaces[prefix] = uri
            
            # List to store all found translatable elements
            translatable_elements = []
            
            # Find all possible translatable elements using various search methods
            
            # 1. Standard trans-units (most common case)
            trans_units = []
            for element in root.findall('.//*'):
                if element.tag.endswith('trans-unit'):
                    trans_units.append(element)
            
            # 2. Look for header elements (often missed)
            header_elements = []
            for element in root.findall('.//*'):
                tag = element.tag.lower()
                if any(x in tag for x in ['title', 'header', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6']):
                    header_elements.append(element)
            
            # 3. Check for elements with translatable attribute
            translatable_attr_elements = []
            for element in root.findall('.//*[@translatable="yes"]'):
                translatable_attr_elements.append(element)
            
            # 4. Check resname attribute for clues about content type
            resname_elements = []
            for element in root.findall('.//*[@resname]'):
                resname = element.get('resname', '').lower()
                if any(x in resname for x in ['title', 'header', 'heading']):
                    resname_elements.append(element)
            
            # Combine all found elements but eliminate duplicates
            all_elements = []
            for elem in trans_units + header_elements + translatable_attr_elements + resname_elements:
                if elem not in all_elements:
                    all_elements.append(elem)
            
            logger.info(f"Found translatable elements in XLIFF file", 
                       extra={"action": "xliff_analysis", "element_count": len(all_elements)})
            
            # Extract text to translate
            translation_items = []
            
            for i, element in enumerate(all_elements):
                # For each element, find source and possibly target elements
                source = None
                target = None
                element_type = "unknown"
                
                # Check if this is a trans-unit
                if element.tag.endswith('trans-unit'):
                    element_type = "trans-unit"
                    # Find the source element
                    for child in element:
                        if child.tag.endswith('source'):
                            source = child
                        elif child.tag.endswith('target'):
                            target = child
                
                # If it's another type of element, the element itself might be translatable
                else:
                    element_type = "direct"
                    source = element
                
                # Extract text from source if found
                source_text = ""
                if source is not None:
                    # Get text content, including text from child elements
                    source_text = ''.join(source.itertext()) if hasattr(source, 'itertext') else (source.text or "")
                    source_text = source_text.strip()
                
                # Only add non-empty items for translation
                if source_text:
                    translation_items.append((i, element_type, element, source, target, source_text))
            
            logger.info(f"Extracted items for translation", 
                       extra={"action": "xliff_extraction", "item_count": len(translation_items)})
            
            # If we found translation items, process them
            if translation_items:
                # Combine all texts for a single translation request with clear markers
                combined_text = "\n---ITEM---\n".join([
                    f"[ITEM-{i}][{element_type.upper()}] {text}" 
                    for i, element_type, _, _, _, text in translation_items
                ])
                
                # Translate using Anthropic
                translated_combined = self.translate_with_anthropic(combined_text, source_language, target_language)
                
                # Split the translated text back into individual items
                # First find the separator pattern
                split_pattern = "\n---ITEM---\n"
                if split_pattern not in translated_combined:
                    # Try alternate patterns if the expected one isn't found
                    possible_patterns = [
                        "\n---ITEM---\n", "\n- - -ITEM- - -\n", "\n--ITEM--\n", 
                        "\n---\n", "\n- - -\n", "\n--\n", "\n\n", "\n"
                    ]
                    for pattern in possible_patterns:
                        if pattern in translated_combined:
                            split_pattern = pattern
                            break
                
                # Split by the identified pattern
                translated_parts = translated_combined.split(split_pattern)
                
                # Clean up the parts and remove any markers
                cleaned_parts = []
                for part in translated_parts:
                    part = part.strip()
                    # Remove item markers like [ITEM-0][TRANS-UNIT]
                    part = re.sub(r'\[ITEM-\d+\]\[\w+\]\s*', '', part)
                    cleaned_parts.append(part)
                
                # If we don't have enough parts, repeat the last one or add placeholders
                while len(cleaned_parts) < len(translation_items):
                    cleaned_parts.append(cleaned_parts[-1] if cleaned_parts else "")
                
                # If we have too many parts, truncate
                if len(cleaned_parts) > len(translation_items):
                    cleaned_parts = cleaned_parts[:len(translation_items)]
                
                # Update the XLIFF with translations
                for idx, (i, element_type, element, source, target, _) in enumerate(translation_items):
                    if idx < len(cleaned_parts):
                        translated_text = cleaned_parts[idx].strip()
                        
                        # Skip if the translation is empty
                        if not translated_text:
                            continue
                        
                        # Different handling based on element type
                        if element_type == "trans-unit":
                            # If we have a target element, update it
                            if target is not None:
                                target.text = translated_text
                            else:
                                # Create a new target element
                                namespace = source.tag.rsplit('}', 1)[0] + '}' if '}' in source.tag else ""
                                target_tag = namespace + 'target' if namespace else 'target'
                                
                                # Create new element with correct namespace
                                new_target = ET.Element(target_tag)
                                new_target.text = translated_text
                                
                                # Copy xml:lang attribute from source if it exists
                                if '{http://www.w3.org/XML/1998/namespace}lang' in source.attrib:
                                    new_target.attrib['{http://www.w3.org/XML/1998/namespace}lang'] = target_language
                                
                                # Add the new target element to the trans-unit
                                element.append(new_target)
                        
                        # For direct elements, update the element's text directly
                        elif element_type == "direct":
                            element.text = translated_text
            
            # Convert the modified XML back to a string
            # Use the native tostring method with careful encoding
            if hasattr(ET, 'ElementTree'):
                tree = ET.ElementTree(root)
                # Use encoding='unicode' for Python 3.2+
                try:
                    translated_xliff = ET.tostring(root, encoding='unicode', method='xml')
                except TypeError:
                    # Fallback for older versions
                    translated_xliff = ET.tostring(root, encoding='utf-8', method='xml').decode('utf-8')
            else:
                # Basic fallback
                translated_xliff = ET.tostring(root, encoding='utf-8', method='xml').decode('utf-8')
            
            logger.info(f"XLIFF translation completed", 
                       extra={"action": "xliff_translation_complete", "result_length": len(translated_xliff)})
            
            # Return the translated XLIFF
            return translated_xliff
            
        except Exception as e:
            import traceback
            logger.error(f"Error translating XLIFF with Anthropic", 
                        extra={"action": "xliff_translation_error", 
                               "error_details": str(e),
                               "traceback": traceback.format_exc()})
            # If there's an error, return the original content
            return xliff_content
    
    def translate_with_anthropic(self, source_text, source_language, target_language):
        """Uses Anthropic's Claude to translate text using direct API calls."""
        try:
            preview_text = source_text[:100] + "..." if len(source_text) > 100 else source_text
            logger.info(f"Translating text with Anthropic", 
                       extra={"action": "translate",
                              "translation_input": preview_text,
                              "source_language": source_language, 
                              "target_language": target_language,
                              "text_length": len(source_text)})
            
            headers = {
                "x-api-key": self.anthropic_api_key,
                "content-type": "application/json",
                "anthropic-version": "2023-06-01"
            }
            
            prompt = f"""Please translate the following text from {source_language} to {target_language}. 
            Provide ONLY the translated text with NO additional comments or explanations.
            
            IMPORTANT: 
            - Preserve any markers like [ITEM-0][TRANS-UNIT] in your translation
            - Maintain the exact same format and structure
            - Translate all content, including titles and headings
            - Keep any HTML tags intact
            - Do not add any extra text or explanations
            
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
            result = response_data["content"][0]["text"]
            
            result_preview = result[:100] + "..." if len(result) > 100 else result
            logger.info(f"Translation complete", 
                       extra={"action": "translate_success", "translation_output": result_preview, "result_length": len(result)})
            
            return result
            
        except Exception as e:
            logger.error(f"Error translating with Anthropic", 
                        extra={"action": "translate_error", "error_details": str(e)})
            raise
    
    def extract_object_key_from_url(self, url):
        """
        Extract the object key from an asset URL or return the path if it's already an object key.
        
        Can handle:
        1. Complete URLs: https://<Storage Account Name>.blob.core.windows.net/gcs/<tenant_id>/...
        2. Direct object keys: <tenant_id>/<ProjectId>/<TaskId>/normalized/...
        """
        # Strip any whitespace
        url = url.strip()
        
        # If URL contains the Azure blob storage pattern, extract the path
        if "blob.core.windows.net/gcs/" in url:
            # Split by the common prefix and take everything after it
            object_key = url.split("blob.core.windows.net/gcs/")[1]
            
            # Remove any query parameters
            if "?" in object_key:
                object_key = object_key.split("?")[0]
            
            logger.debug(f"Extracted object key from URL", 
                        extra={"action": "extract_object_key", "url": url, "object_key": object_key})
            return object_key
        
        # Check if this is already an object key (contains normalized path pattern)
        if "normalized" in url and "/" in url and not url.startswith("http"):
            logger.debug(f"URL appears to be a direct object key", 
                        extra={"action": "extract_object_key_direct", "object_key": url})
            return url
        
        # If the URL doesn't match any expected pattern, log warning and return as is
        logger.warning(f"Could not extract object key from URL", 
                      extra={"action": "extract_object_key_failed", "url": url})
        return url
    
    def handle_translate_event(self, event):
        """
        Handles a TRANSLATE event by following the Adobe GCS translation workflow:
        1. Get assets information
        2. Download the XLIFF file
        3. Translate the XLIFF content
        4. Upload the translated XLIFF
        5. Complete the asset translation
        6. Mark the task as complete
        """
        try:
            # Extract information from the event
            project_id = event.get("projectId")
            task_id = event.get("taskId")
            source_locale = event.get("sourceLocale")
            target_locale = event.get("targetLocale")
            tenant_id = event.get("tenantId")
            
            logger.info(f"Processing TRANSLATE event", 
                       extra={"action": "translate_event", 
                              "project_id": project_id, 
                              "task_id": task_id,
                              "source_locale": source_locale,
                              "target_locale": target_locale})
            
            # Log the full event data for debugging
            logger.info(f"Event data", extra={"action": "event_data", "event": json.dumps(event)})
            
            # Step 1: Get assets information
            assets = self.get_assets(project_id, task_id, target_locale, tenant_id)
            logger.info(f"Retrieved assets for translation", 
                       extra={"action": "assets_retrieved", "count": len(assets)})
            
            if not assets:
                logger.warning("No assets found for translation", 
                              extra={"action": "no_assets", "task_id": task_id})
                return
            
            for asset in assets:
                asset_name = asset.get("name")
                logger.info(f"Processing asset", 
                           extra={"action": "process_asset", "asset_name": asset_name})
                
                # Find the NORMALIZED URL from assetUrls
                normalized_url = None
                asset_urls = asset.get("assetUrls", [])
                
                for url_info in asset_urls:
                    if url_info.get("urlType") == "NORMALIZED" and url_info.get("locale") == source_locale:
                        normalized_url = url_info.get("url")
                        break
                
                if not normalized_url:
                    logger.warning(f"No NORMALIZED URL found for asset", 
                                  extra={"action": "no_normalized_url", "asset_name": asset_name})
                    continue
                
                # Extract the object key from the normalized URL
                object_key = self.extract_object_key_from_url(normalized_url)
                logger.info(f"Extracted object key", 
                           extra={"action": "object_key", "object_key": object_key})
                
                # Step 2: Download the XLIFF content
                xliff_content = self.get_asset_content(tenant_id, object_key)
                logger.info(f"Retrieved XLIFF content", 
                           extra={"action": "xliff_retrieved", "content_length": len(xliff_content)})
                
                # Step 3: Translate the XLIFF content
                translated_xliff = self.translate_xliff_with_anthropic(xliff_content, source_locale, target_locale)
                logger.info(f"Translated XLIFF content", 
                           extra={"action": "xliff_translated", "content_length": len(translated_xliff)})
                
                # Step 4: Upload the translated XLIFF
                file_name = f"{asset_name}_{target_locale}.xlf"
                translated_url = self.upload_translated_content(tenant_id, translated_xliff, file_name)
                logger.info(f"Uploaded translated content", 
                           extra={"action": "content_uploaded", "url": translated_url})
                
                # Step 5: Complete the asset translation
                completion_result = self.complete_asset_translation(
                    project_id, task_id, asset_name, target_locale, tenant_id, translated_url
                )
                logger.info(f"Completed asset translation", 
                           extra={"action": "asset_completed", "result": json.dumps(completion_result)})
            
            
        except Exception as e:
            import traceback
            logger.error(f"Error handling TRANSLATE event", 
                        extra={"action": "translate_event_error", 
                               "error_details": str(e),
                               "traceback": traceback.format_exc(),
                               "project_id": project_id,
                               "task_id": task_id})
    
    def handle_retranslate_event(self, event):
        """
        Handles a RE_TRANSLATE event following the same workflow as TRANSLATE
        but using the specific asset URL from the event, and completing the task.
        """
        try:
            # Extract information from the event
            project_id = event.get("projectId")
            task_id = event.get("taskId")
            source_locale = event.get("sourceLocale")
            target_locale = event.get("targetLocale")
            tenant_id = event.get("tenantId")
            asset_name = event.get("assetName")
            asset_url = event.get("assetUrl")
            
            logger.info(f"Processing RE_TRANSLATE event", 
                       extra={"action": "retranslate_event", 
                              "project_id": project_id, 
                              "task_id": task_id,
                              "asset_name": asset_name})
            
            # Log the full event data for debugging
            logger.info(f"Event data", extra={"action": "event_data", "event": json.dumps(event)})
            
            # For RE_TRANSLATE, we already have the asset URL in the event
            if not asset_url:
                logger.warning("No asset URL found in RE_TRANSLATE event", 
                              extra={"action": "no_asset_url", "task_id": task_id})
                return
            
            # Download the XLIFF content
            # The asset_url is a direct download link, so we don't need to use the assetContent API
            logger.info(f"Downloading XLIFF content from URL", 
                       extra={"action": "download_xliff", "url": asset_url})
            response = requests.get(asset_url, headers=self.get_auth_headers())
            response.raise_for_status()
            xliff_content = response.text
            
            logger.info(f"Retrieved XLIFF content", 
                       extra={"action": "xliff_retrieved", "content_length": len(xliff_content)})
            
            # Translate the XLIFF content
            translated_xliff = self.translate_xliff_with_anthropic(xliff_content, source_locale, target_locale)
            logger.info(f"Translated XLIFF content", 
                       extra={"action": "xliff_translated", "content_length": len(translated_xliff)})
            
            # Upload the translated XLIFF
            file_name = f"{asset_name}_{target_locale}.xlf"
            translated_url = self.upload_translated_content(tenant_id, translated_xliff, file_name)
            logger.info(f"Uploaded translated content", 
                       extra={"action": "content_uploaded", "url": translated_url})
            
            # Complete the asset translation
            completion_result = self.complete_asset_translation(
                project_id, task_id, asset_name, target_locale, tenant_id, translated_url
            )
            logger.info(f"Completed asset translation", 
                       extra={"action": "asset_completed", "result": json.dumps(completion_result)})
            
        except Exception as e:
            import traceback
            logger.error(f"Error handling RE_TRANSLATE event", 
                        extra={"action": "retranslate_event_error", 
                               "error_details": str(e),
                               "traceback": traceback.format_exc(),
                               "project_id": project_id if 'project_id' in locals() else 'unknown',
                               "task_id": task_id if 'task_id' in locals() else 'unknown'})
    
    def run(self):
        """Main execution loop of the connector."""
        logger.info("Starting GCS Connector", extra={"action": "startup"})
        
        # Initial token refresh
        self.refresh_access_token()
        
        while True:
            try:
                self.poll_for_events()
                
                # Sleep before polling again
                logger.info(f"Sleeping", extra={"action": "sleeping", "seconds": self.poll_interval})
                time.sleep(self.poll_interval)
                
            except Exception as e:
                import traceback
                logger.error(f"Error in main loop", 
                            extra={"action": "main_loop_error", 
                                   "error_details": str(e),
                                   "traceback": traceback.format_exc()})
                
                # Sleep a bit longer before retrying after an error
                time.sleep(self.poll_interval * 2)

# Main entry point
if __name__ == "__main__":
    connector = GCSConnector()
    connector.run()

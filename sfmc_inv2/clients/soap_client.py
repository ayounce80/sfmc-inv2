"""SOAP API client for SFMC.

Provides XML envelope construction, response parsing, and pagination support
for SFMC's SOAP API endpoints.
"""

import asyncio
import logging
import os
import time
from typing import Any, Optional
from xml.etree import ElementTree as ET
from xml.etree.ElementTree import Element

import httpx

from ..core.config import SFMCConfig, get_config
from .auth import TokenManager, get_token_manager

logger = logging.getLogger(__name__)

# XML Namespaces
SOAP_ENV = "http://schemas.xmlsoap.org/soap/envelope/"
ET_NS = "http://exacttarget.com/wsdl/partnerAPI"
XSI_NS = "http://www.w3.org/2001/XMLSchema-instance"

# Register namespaces
ET.register_namespace("soap", SOAP_ENV)
ET.register_namespace("", ET_NS)
ET.register_namespace("xsi", XSI_NS)

# Retry configuration
MAX_RETRIES = 3
RETRY_DELAY = 1.0
RETRY_BACKOFF = 2.0
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

# Default maximum pages for pagination safety (can be overridden via config)
DEFAULT_MAX_PAGES = 100


def env_with_oauth(access_token: str) -> Element:
    """Build a SOAP envelope with OAuth token in header.

    Args:
        access_token: Valid OAuth access token.

    Returns:
        Element: SOAP Envelope with fueloauth header and empty Body.
    """
    envelope = ET.Element(f"{{{SOAP_ENV}}}Envelope")

    # Header with OAuth token
    header = ET.SubElement(envelope, f"{{{SOAP_ENV}}}Header")
    fueloauth = ET.SubElement(header, f"{{{ET_NS}}}fueloauth")
    fueloauth.text = access_token

    # Empty body for caller to populate
    ET.SubElement(envelope, f"{{{SOAP_ENV}}}Body")

    return envelope


def build_retrieve_request(
    object_type: str,
    properties: list[str],
    filter_xml: Optional[Element] = None,
) -> Element:
    """Build a SOAP RetrieveRequest element.

    Args:
        object_type: SFMC object type (e.g., "Automation", "DataExtension")
        properties: List of property names to retrieve
        filter_xml: Optional Filter element for the request

    Returns:
        RetrieveRequestMsg element ready to be added to envelope Body.
    """
    msg = ET.Element(f"{{{ET_NS}}}RetrieveRequestMsg")
    req = ET.SubElement(msg, f"{{{ET_NS}}}RetrieveRequest")

    obj_type = ET.SubElement(req, f"{{{ET_NS}}}ObjectType")
    obj_type.text = object_type

    for prop in properties:
        prop_elem = ET.SubElement(req, f"{{{ET_NS}}}Properties")
        prop_elem.text = prop

    if filter_xml is not None:
        req.append(filter_xml)

    return msg


def build_continue_request(request_id: str) -> Element:
    """Build a SOAP ContinueRequest for pagination.

    Args:
        request_id: RequestID from previous response.

    Returns:
        RetrieveRequestMsg element for continuation.
    """
    msg = ET.Element(f"{{{ET_NS}}}RetrieveRequestMsg")
    req = ET.SubElement(msg, f"{{{ET_NS}}}RetrieveRequest")

    continue_req = ET.SubElement(req, f"{{{ET_NS}}}ContinueRequest")
    continue_req.text = request_id

    return msg


def build_simple_filter(
    property_name: str,
    operator: str,
    value: str,
) -> Element:
    """Build a simple SOAP filter element.

    Args:
        property_name: Property to filter on
        operator: Filter operator (equals, like, greaterThan, etc.)
        value: Filter value

    Returns:
        Filter element.
    """
    filter_elem = ET.Element(f"{{{ET_NS}}}Filter")
    filter_elem.set(f"{{{XSI_NS}}}type", "SimpleFilterPart")

    prop = ET.SubElement(filter_elem, f"{{{ET_NS}}}Property")
    prop.text = property_name

    op = ET.SubElement(filter_elem, f"{{{ET_NS}}}SimpleOperator")
    op.text = operator

    val = ET.SubElement(filter_elem, f"{{{ET_NS}}}Value")
    val.text = value

    return filter_elem


def parse_soap_response(response_xml: str) -> dict[str, Any]:
    """Parse a SOAP response into a dictionary.

    Extracts common response fields: OverallStatus, ObjectID, StatusMessage,
    ErrorCode, TaskID.

    Args:
        response_xml: Raw XML response string.

    Returns:
        Dict with parsed response data.
    """
    result: dict[str, Any] = {
        "ok": False,
        "overall_status": None,
        "request_id": None,
        "results": [],
    }

    try:
        root = ET.fromstring(response_xml)

        # Find Body element
        body = root.find(f".//{{{SOAP_ENV}}}Body")
        if body is None:
            result["error"] = "No SOAP Body found"
            return result

        # Check for fault
        fault = body.find(f".//{{{SOAP_ENV}}}Fault")
        if fault is not None:
            fault_string = fault.find("faultstring")
            result["error"] = fault_string.text if fault_string is not None else "SOAP Fault"
            return result

        # Find response element (varies by operation)
        for child in body:
            # Extract OverallStatus
            status = child.find(f".//{{{ET_NS}}}OverallStatus")
            if status is not None:
                result["overall_status"] = status.text
                result["ok"] = status.text in ("OK", "MoreDataAvailable")

            # Extract RequestID for pagination
            req_id = child.find(f".//{{{ET_NS}}}RequestID")
            if req_id is not None:
                result["request_id"] = req_id.text

            # Extract Results
            results = child.findall(f".//{{{ET_NS}}}Results")
            for res in results:
                result["results"].append(_element_to_dict(res))

    except ET.ParseError as e:
        result["error"] = f"XML parse error: {e}"

    return result


def parse_retrieve_response(response_xml: str) -> dict[str, Any]:
    """Parse a SOAP RetrieveResponse specifically.

    Optimized for Retrieve operations with object list extraction.

    Args:
        response_xml: Raw XML response string.

    Returns:
        Dict with ok, overall_status, request_id, objects list.
    """
    result: dict[str, Any] = {
        "ok": False,
        "overall_status": None,
        "request_id": None,
        "objects": [],
    }

    try:
        root = ET.fromstring(response_xml)

        # Navigate to RetrieveResponseMsg
        body = root.find(f".//{{{SOAP_ENV}}}Body")
        if body is None:
            result["error"] = "No SOAP Body found"
            return result

        # Check for fault
        fault = body.find(f".//{{{SOAP_ENV}}}Fault")
        if fault is not None:
            fault_string = fault.find("faultstring")
            result["error"] = fault_string.text if fault_string is not None else "SOAP Fault"
            return result

        # Find RetrieveResponseMsg
        response_msg = body.find(f".//{{{ET_NS}}}RetrieveResponseMsg")
        if response_msg is None:
            # Try without namespace
            response_msg = body.find(".//RetrieveResponseMsg")

        if response_msg is not None:
            # Overall status
            status = response_msg.find(f"{{{ET_NS}}}OverallStatus")
            if status is None:
                status = response_msg.find("OverallStatus")
            if status is not None:
                result["overall_status"] = status.text
                result["ok"] = status.text in ("OK", "MoreDataAvailable")

            # Request ID for pagination
            req_id = response_msg.find(f"{{{ET_NS}}}RequestID")
            if req_id is None:
                req_id = response_msg.find("RequestID")
            if req_id is not None:
                result["request_id"] = req_id.text

            # Extract objects
            objects = response_msg.findall(f"{{{ET_NS}}}Results")
            if not objects:
                objects = response_msg.findall("Results")
            for obj in objects:
                result["objects"].append(_element_to_dict(obj))

    except ET.ParseError as e:
        result["error"] = f"XML parse error: {e}"

    return result


def _element_to_dict(element: Element) -> dict[str, Any]:
    """Convert an XML element to a dictionary recursively.

    Args:
        element: XML Element to convert.

    Returns:
        Dictionary representation of the element.
    """
    result: dict[str, Any] = {}

    # Handle attributes
    for key, value in element.attrib.items():
        # Strip namespace from attribute names
        clean_key = key.split("}")[-1] if "}" in key else key
        result[f"@{clean_key}"] = value

    # Handle child elements
    for child in element:
        # Strip namespace from tag
        tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag

        if len(child) == 0 and not child.attrib:
            # Leaf node - just text
            value = child.text
        else:
            # Complex node - recurse
            value = _element_to_dict(child)

        # Handle multiple children with same tag
        if tag in result:
            if not isinstance(result[tag], list):
                result[tag] = [result[tag]]
            result[tag].append(value)
        else:
            result[tag] = value

    return result


class SOAPClient:
    """SOAP API client for SFMC.

    Provides methods for making SOAP requests with automatic retry,
    token refresh, and pagination support.
    """

    def __init__(
        self,
        config: Optional[SFMCConfig] = None,
        token_manager: Optional[TokenManager] = None,
    ):
        """Initialize the SOAP client.

        Args:
            config: SFMC configuration. If None, loads from environment.
            token_manager: Token manager instance. If None, uses default.
        """
        self._config = config or get_config()
        self._token_manager = token_manager or get_token_manager(config)
        self._debug = self._config.soap_debug
        self._max_pages = self._config.soap_max_pages

    @property
    def endpoint(self) -> str:
        """Get the SOAP API endpoint URL."""
        return self._config.soap_url

    def _log_request(self, xml: str) -> None:
        """Log request XML if debug is enabled."""
        if self._debug:
            logger.debug(f"SOAP Request:\n{xml}")

    def _log_response(self, xml: str) -> None:
        """Log response XML if debug is enabled."""
        if self._debug:
            logger.debug(f"SOAP Response:\n{xml[:2000]}")

    def post(self, envelope: Element) -> dict[str, Any]:
        """Make a synchronous SOAP request.

        Args:
            envelope: Complete SOAP envelope element.

        Returns:
            Parsed response dictionary.
        """
        xml_bytes = ET.tostring(envelope, encoding="utf-8", xml_declaration=True)
        xml_str = xml_bytes.decode("utf-8")
        self._log_request(xml_str)

        last_error: Optional[Exception] = None

        for attempt in range(MAX_RETRIES):
            try:
                with httpx.Client(timeout=120) as client:
                    response = client.post(
                        self.endpoint,
                        content=xml_bytes,
                        headers={
                            "Content-Type": "text/xml; charset=utf-8",
                            "SOAPAction": "Retrieve",
                        },
                    )

                    self._log_response(response.text)

                    # Handle retryable HTTP errors
                    if response.status_code in RETRYABLE_STATUS_CODES:
                        delay = RETRY_DELAY * (RETRY_BACKOFF**attempt)
                        logger.debug(
                            f"Got {response.status_code}, retrying in {delay}s "
                            f"(attempt {attempt + 1}/{MAX_RETRIES})"
                        )
                        time.sleep(delay)
                        continue

                    return parse_soap_response(response.text)

            except httpx.TimeoutException as e:
                last_error = e
                logger.debug(f"SOAP timeout (attempt {attempt + 1}/{MAX_RETRIES})")
                time.sleep(RETRY_DELAY * (RETRY_BACKOFF**attempt))

            except httpx.RequestError as e:
                last_error = e
                logger.debug(f"SOAP error: {e} (attempt {attempt + 1}/{MAX_RETRIES})")
                time.sleep(RETRY_DELAY * (RETRY_BACKOFF**attempt))

        return {
            "ok": False,
            "error": str(last_error) if last_error else "Max retries exceeded",
        }

    async def post_async(self, envelope: Element) -> dict[str, Any]:
        """Make an asynchronous SOAP request.

        Args:
            envelope: Complete SOAP envelope element.

        Returns:
            Parsed response dictionary.
        """
        xml_bytes = ET.tostring(envelope, encoding="utf-8", xml_declaration=True)
        xml_str = xml_bytes.decode("utf-8")
        self._log_request(xml_str)

        last_error: Optional[Exception] = None

        for attempt in range(MAX_RETRIES):
            try:
                async with httpx.AsyncClient(timeout=120) as client:
                    response = await client.post(
                        self.endpoint,
                        content=xml_bytes,
                        headers={
                            "Content-Type": "text/xml; charset=utf-8",
                            "SOAPAction": "Retrieve",
                        },
                    )

                    self._log_response(response.text)

                    # Handle retryable HTTP errors
                    if response.status_code in RETRYABLE_STATUS_CODES:
                        delay = RETRY_DELAY * (RETRY_BACKOFF**attempt)
                        logger.debug(
                            f"Got {response.status_code}, retrying in {delay}s "
                            f"(attempt {attempt + 1}/{MAX_RETRIES})"
                        )
                        await asyncio.sleep(delay)
                        continue

                    return parse_soap_response(response.text)

            except httpx.TimeoutException as e:
                last_error = e
                logger.debug(f"SOAP timeout (attempt {attempt + 1}/{MAX_RETRIES})")
                await asyncio.sleep(RETRY_DELAY * (RETRY_BACKOFF**attempt))

            except httpx.RequestError as e:
                last_error = e
                logger.debug(f"SOAP error: {e} (attempt {attempt + 1}/{MAX_RETRIES})")
                await asyncio.sleep(RETRY_DELAY * (RETRY_BACKOFF**attempt))

        return {
            "ok": False,
            "error": str(last_error) if last_error else "Max retries exceeded",
        }

    def retrieve(
        self,
        object_type: str,
        properties: list[str],
        filter_xml: Optional[Element] = None,
    ) -> dict[str, Any]:
        """Make a SOAP Retrieve request.

        Args:
            object_type: SFMC object type to retrieve.
            properties: List of properties to retrieve.
            filter_xml: Optional filter element.

        Returns:
            Parsed response with objects list.
        """
        token = self._token_manager.get_token()
        envelope = env_with_oauth(token)
        body = envelope.find(f".//{{{SOAP_ENV}}}Body")

        retrieve_msg = build_retrieve_request(object_type, properties, filter_xml)
        body.append(retrieve_msg)  # type: ignore

        xml_bytes = ET.tostring(envelope, encoding="utf-8", xml_declaration=True)
        self._log_request(xml_bytes.decode("utf-8"))

        last_error: Optional[Exception] = None

        for attempt in range(MAX_RETRIES):
            try:
                with httpx.Client(timeout=120) as client:
                    response = client.post(
                        self.endpoint,
                        content=xml_bytes,
                        headers={
                            "Content-Type": "text/xml; charset=utf-8",
                            "SOAPAction": "Retrieve",
                        },
                    )

                    self._log_response(response.text)

                    if response.status_code == 401:
                        self._token_manager.force_refresh()
                        # Rebuild envelope with new token
                        token = self._token_manager.get_token()
                        envelope = env_with_oauth(token)
                        body = envelope.find(f".//{{{SOAP_ENV}}}Body")
                        body.append(build_retrieve_request(object_type, properties, filter_xml))  # type: ignore
                        xml_bytes = ET.tostring(envelope, encoding="utf-8", xml_declaration=True)
                        continue

                    if response.status_code in RETRYABLE_STATUS_CODES:
                        delay = RETRY_DELAY * (RETRY_BACKOFF**attempt)
                        time.sleep(delay)
                        continue

                    return parse_retrieve_response(response.text)

            except (httpx.TimeoutException, httpx.RequestError) as e:
                last_error = e
                time.sleep(RETRY_DELAY * (RETRY_BACKOFF**attempt))

        return {
            "ok": False,
            "error": str(last_error) if last_error else "Max retries exceeded",
            "objects": [],
        }

    def retrieve_all_pages(
        self,
        object_type: str,
        properties: list[str],
        filter_xml: Optional[Element] = None,
        max_pages: Optional[int] = None,
    ) -> dict[str, Any]:
        """Retrieve all pages of objects using pagination.

        Args:
            object_type: SFMC object type to retrieve.
            properties: List of properties to retrieve.
            filter_xml: Optional filter element.
            max_pages: Maximum number of pages to retrieve. Defaults to config value.

        Returns:
            Combined response with all objects.
        """
        if max_pages is None:
            max_pages = self._max_pages
        all_objects: list[dict[str, Any]] = []
        page = 1

        # First request
        result = self.retrieve(object_type, properties, filter_xml)
        if not result.get("ok"):
            return result

        all_objects.extend(result.get("objects", []))

        # Pagination loop
        while result.get("overall_status") == "MoreDataAvailable" and page < max_pages:
            request_id = result.get("request_id")
            if not request_id:
                break

            page += 1
            token = self._token_manager.get_token()
            envelope = env_with_oauth(token)
            body = envelope.find(f".//{{{SOAP_ENV}}}Body")
            body.append(build_continue_request(request_id))  # type: ignore

            xml_bytes = ET.tostring(envelope, encoding="utf-8", xml_declaration=True)

            try:
                with httpx.Client(timeout=120) as client:
                    response = client.post(
                        self.endpoint,
                        content=xml_bytes,
                        headers={
                            "Content-Type": "text/xml; charset=utf-8",
                            "SOAPAction": "Retrieve",
                        },
                    )
                    result = parse_retrieve_response(response.text)

                    if result.get("ok"):
                        all_objects.extend(result.get("objects", []))
                    else:
                        break

            except Exception as e:
                logger.error(f"Pagination error on page {page}: {e}")
                break

        return {
            "ok": True,
            "objects": all_objects,
            "pages_retrieved": page,
        }


# Module-level convenience functions
_default_client: Optional[SOAPClient] = None


def get_soap_client(config: Optional[SFMCConfig] = None) -> SOAPClient:
    """Get or create the default SOAP client."""
    global _default_client
    if _default_client is None:
        _default_client = SOAPClient(config)
    return _default_client


def reset_soap_client() -> None:
    """Reset the default SOAP client singleton."""
    global _default_client
    _default_client = None

import requests
from bs4 import BeautifulSoup
import json
import re
from readability import Document # Requires: pip install readability-lxml

# --- Part 1: HTML Fetching (Using requests for your environment) ---
def fetch_html_content(url: str) -> str | None:
    """
    Fetches the HTML content of a given URL using the requests library.

    Args:
        url (str): The URL to fetch.

    Returns:
        str | None: The HTML content as a string, or None if fetching fails.
    """
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while fetching {url} with requests: {e}")
        return None

# --- Part 2: Advanced Beautiful Soup Parser ---
def parse_daz3d_product_page_robust(html_content: str) -> dict:
    """
    Parses the HTML content of a Daz3D product page to extract specific metadata
    and the main narrative text using a combination of JSON-LD, targeted Beautiful Soup,
    and readability-lxml.

    Args:
        html_content (str): The HTML content of the product page.

    Returns:
        dict: A dictionary containing all extracted product information,
              including raw JSON-LD data for internal processing.
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    product_info = {
        "Artists": "N/A",
        "Compatible Figures": "N/A",
        "Compatible Software": "N/A",
        "Product Details and Description": "N/A",
        "Main Narrative Text (from Readability)": "N/A", # General content
        "JSON-LD Data": {} # To store any extracted JSON-LD directly for internal processing
    }

    # --- 1. Extract from JSON-LD (Schema.org) if available ---
    json_ld_scripts = soup.find_all('script', type='application/ld+json')
    for script in json_ld_scripts:
        try:
            data = json.loads(script.string)
            # Handle cases where JSON-LD is a list of objects or a single object
            product_data = None
            if isinstance(data, list):
                product_data = next((item for item in data if isinstance(item, dict) and item.get('@type') == 'Product'), None)
                if product_data:
                    product_info["JSON-LD Data"] = product_data
                else: # Fallback if no explicit Product schema is found in a list
                    product_info["JSON-LD Data"] = data[0] if data else {}
            elif isinstance(data, dict):
                product_data = data
                product_info["JSON-LD Data"] = data

            # Populate fields from the identified Product JSON-LD
            if product_data and product_data.get('@type') == 'Product':
                if product_data.get('manufacturer') and isinstance(product_data['manufacturer'], dict) and product_data['manufacturer'].get('name'):
                    product_info["Artists"] = product_data['manufacturer']['name'].strip()
                if product_data.get('description') and product_info["Product Details and Description"] == "N/A":
                    product_info["Product Details and Description"] = product_data['description'].strip()
        except json.JSONDecodeError:
            pass # Ignore scripts that are not valid JSON
        except Exception as e:
            pass # Catch other potential errors during JSON-LD processing

    # --- 2. Targeted Beautiful Soup Parsing (for metadata not found in JSON-LD or as a fallback) ---

    # This helper function specifically targets a container (like span/div/p)
    # that has a bold label and extracts the value immediately following it.
    def extract_value_from_labeled_container(label_text: str, target_key: str):
        # Find all spans, divs, or paragraphs that could contain the label
        # We look for a container whose *direct text children* or *text of specific internal tags*
        # matches the pattern.
        
        # More robust: Find the <b> tag with the specific label text
        bold_tag = soup.find('b', string=lambda s: s and s.strip() == label_text)

        if bold_tag:
            # The value is usually the next sibling of the <b> tag,
            # or it's part of the text content of the <b> tag's parent.
            
            # Scenario 1: Value is next_sibling text
            next_sibling = bold_tag.next_sibling
            if next_sibling and isinstance(next_sibling, str):
                value = next_sibling.strip()
                # Ensure it's not just whitespace or another label starts immediately
                if value and not value.startswith("Compatible") and not value.startswith("Install"):
                    product_info[target_key] = value
                    return

            # Scenario 2: Value is part of the parent's text, but after the bold tag
            # This is complex to do without over-grabbing. Let's try to extract
            # the text from the parent, then strip the bold tag's text from it.
            parent_text = bold_tag.parent.get_text(separator=" ", strip=True) if bold_tag.parent else ""
            if parent_text:
                match = re.search(rf"{re.escape(label_text)}\s*(.*)", parent_text)
                if match:
                    value = match.group(1).strip()
                    if value and not value.startswith("Compatible") and not value.startswith("Install"): # Prevent over-grabbing
                        product_info[target_key] = value
                        return

    # Artist (Only if not found in JSON-LD)
    if product_info["Artists"] == "N/A":
        extract_value_from_labeled_container("Artist:", "Artists")

    # Compatible Figures
    extract_value_from_labeled_container("Compatible Figures:", "Compatible Figures")

    # Compatible Software
    extract_value_from_labeled_container("Compatible Software:", "Compatible Software")


    # For "Product Details and Description" (if not found in JSON-LD)
    if product_info["Product Details and Description"] == "N/A":
        # First, try to find a specific div/p with a known class or ID for description (most reliable)
        # However, without inspecting current live HTML, we rely on text patterns.

        # Method 1: Find a specific heading "Details" and its following content
        # Broaden search for heading tags
        details_heading = soup.find(lambda tag: tag.name in ['h2', 'h3', 'strong', 'span', 'p', 'div'] and tag.get_text(strip=True).lower() == "details")
        if details_heading:
            description_parts = []
            current_element = details_heading.find_next_sibling()
            # Stop when another significant heading, section, or irrelevant block starts
            while current_element and current_element.name not in ['h2', 'h3', 'section', 'aside', 'footer']:
                text_content = current_element.get_text(separator="\n", strip=True)
                # Heuristic: Filter out very short texts, menus, or common irrelevant phrases
                if text_content and len(text_content) > 50 and \
                   not any(phrase in text_content.lower() for phrase in ["download studio", "add to cart", "reviews", "features", "support"]):
                    description_parts.append(text_content)
                current_element = current_element.find_next_sibling()
            if description_parts:
                product_info["Product Details and Description"] = "\n\n".join(description_parts)
        
        # Fallback Method 2: Look for the characteristic starting phrase directly in a block
        if product_info["Product Details and Description"] == "N/A":
            description_block = soup.find(
                lambda tag: tag.name in ['div', 'p', 'span'] and
                "Reclining Poses for Genesis 9 by Handspan Studios" in tag.get_text(strip=True)
            )
            if description_block:
                product_info["Product Details and Description"] = description_block.get_text(separator="\n", strip=True)


    # --- 3. Extract Main Narrative Text using readability-lxml ---
    try:
        doc = Document(html_content)
        readable_html = doc.summary()
        readable_soup = BeautifulSoup(readable_html, 'html.parser')
        main_text = readable_soup.get_text(separator="\n", strip=True)
        
        # Clean up common remnants (like header/footer text that readability might miss)
        main_text = re.sub(r'JavaScript seems to be disabled in your browser\..*', '', main_text, flags=re.DOTALL | re.IGNORECASE).strip()
        main_text = re.sub(r'You must have JavaScript enabled in your browser to utilize the functionality of this website\..*', '', main_text, flags=re.DOTALL | re.IGNORECASE).strip()
        main_text = re.sub(r'\s{2,}', '\n\n', main_text) # Normalize multiple spaces/newlines
        
        product_info["Main Narrative Text (from Readability)"] = main_text

    except Exception as e:
        product_info["Main Narrative Text (from Readability)"] = f"Could not extract with readability-lxml: {e}"

    return product_info

# --- Example Usage ---
if __name__ == "__main__":
    target_url = "https://www.daz3d.com/reclining-poses-for-genesis-9"

    print(f"Fetching and parsing {target_url}...")
    html_content = fetch_html_content(target_url)

    if html_content:
        extracted_data = parse_daz3d_product_page_robust(html_content)

        # Filter the extracted data to only include the desired keys for the final JSON output
        final_output = {
            "Artists": extracted_data.get("Artists", "N/A"),
            "Compatible Figures": extracted_data.get("Compatible Figures", "N/A"),
            "Compatible Software": extracted_data.get("Compatible Software", "N/A"),
            "Product Details and Description": extracted_data.get("Product Details and Description", "N/A"),
            "Main Narrative Text (from Readability)": extracted_data.get("Main Narrative Text (from Readability)", "N/A")
            # Uncomment the line below if you also want the raw JSON-LD data in the final output
            # "Raw JSON-LD Data": extracted_data.get("JSON-LD Data", {})
        }

        # Print the final output as a pretty-printed JSON string
        print("\n--- Extracted Product Information (JSON) ---")
        print(json.dumps(final_output, indent=2))
        print("------------------------------------------")

    else:
        print(f"Failed to fetch HTML content from {target_url}.")
import pandas as pd
import requests
import sys
import time
import json
import warnings
from urllib3.exceptions import InsecureRequestWarning

# Suppress only the InsecureRequestWarning from urllib3
warnings.simplefilter('ignore', InsecureRequestWarning)

def get_author_name(author_id):
    """Fetches an author's name from the OpenAlex API."""
    url = f"https://api.openalex.org/authors/{author_id}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json().get('display_name')
    except requests.exceptions.RequestException:
        # Return None if there's any issue, will be skipped later
        return None

def find_thesis_by_name(author_name):
    """
    Queries the theses.fr API and returns the URL and data if a match is found.
    """
    if not author_name:
        return None, None, None

    search_url = "https://theses.fr/api/v1/personnes/recherche/"
    params = {'q': author_name}
    
    # Prepare the URL for printing before the request is made
    request = requests.Request('GET', search_url, params=params)
    prepared_request = request.prepare()
    request_url = prepared_request.url

    try:
        response = requests.get(search_url, params=params, verify=False, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        # A match is considered found if the 'personnes' list exists and is not empty
        if data.get('personnes'):
            return True, request_url, data
        else:
            return False, request_url, None

    except (requests.exceptions.RequestException, json.JSONDecodeError):
        return False, request_url, None

def main():
    """
    Main function to read authors from a CSV, filter for those starting in France,
    and check if they have a thesis record.
    """
    try:
        df = pd.read_csv('author_sequences_final.py.csv')
    except FileNotFoundError:
        print("Error: 'author_sequences_final.py.csv' not found.", file=sys.stderr)
        print("Please run 'process_authors.py' first.", file=sys.stderr)
        sys.exit(1)

    print(f"Starting to process {len(df)} total authors from the input file...")
    
    authors_to_check_count = 0
    matches_found_count = 0
    debug_prints = 0

    # To process a small sample for testing, uncomment the following line
    # df = df.head(500)

    for index, row in df.iterrows():
        # Check for both possible column names to be robust
        country_sequence = row.get('country_sequence', row.get('country_codes_sequence', ''))

        # Handle cases where the sequence might be missing or not a string
        if not isinstance(country_sequence, str) or not country_sequence:
            continue
            
        # Debugging: Print sequences that contain 'FR' to see why they aren't matching
        if 'FR' in country_sequence and debug_prints < 10:
            print(f"\n[DEBUG] Row {index}:")
            print(f"  Raw country_sequence: '{country_sequence}'")

        # Find the first non-empty country in the sequence. The separator is ' -> '.
        countries = [c.strip() for c in country_sequence.split(' -> ')]
        first_country = None
        for country in countries:
            # Handle different kinds of empty placeholders
            country_lower = country.lower()
            if country and country_lower != 'empty' and country_lower != '<empty>':
                # A field can contain multiple countries like 'FR,GB'.
                # We only care about the first one in that list for the first publication.
                first_country = country.split(',')[0].strip()
                break
        
        if 'FR' in country_sequence and debug_prints < 10:
            print(f"  Detected first_country: '{first_country}'")
            debug_prints += 1
            
        # Only proceed if the first valid country is 'FR'
        if first_country != 'FR':
            continue

        authors_to_check_count += 1
        author_id = row['author_id']
        
        author_name = get_author_name(author_id)
        
        if not author_name:
            # Silently skip if name can't be fetched. Uncomment to debug.
            # print(f"Skipping Author ID {author_id} (France-starter): Could not fetch name.", file=sys.stderr)
            continue
            
        has_match, url, results = find_thesis_by_name(author_name)
        
        if has_match:
            matches_found_count += 1
            print("\n" + "="*20 + " MATCH FOUND " + "="*20)
            print(f"Author ID:   {author_id}")
            print(f"Request URL: {url}")
            print("API Results:")
            print(json.dumps(results, indent=2, ensure_ascii=False))
            print("="*53 + "\n")
        
        # Be respectful to the APIs by adding a short delay
        time.sleep(0.2)
        
    print("\n" + "="*60)
    print("Processing Complete.")
    print(f"Filtered to {authors_to_check_count} authors whose first non-empty country was 'FR'.")
    print(f"Found {matches_found_count} potential PhD matches among them.")
    print("="*60)


if __name__ == '__main__':
    main() 
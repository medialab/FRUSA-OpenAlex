import requests
from bs4 import BeautifulSoup
import argparse
import sys

def get_author_details(author_id):
    """
    Fetches the author's display name, concepts, and institution from the OpenAlex API.
    
    Args:
        author_id (str): The OpenAlex author ID (e.g., A5000002327).
        
    Returns:
        dict: A dictionary with author details, or None if not found.
    """
    url = f"https://api.openalex.org/authors/{author_id}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        author_details = {
            'display_name': data.get('display_name'),
            'institution': data.get('last_known_institution', {}).get('display_name') if data.get('last_known_institution') else None,
            'concepts': [concept.get('display_name').lower() for concept in data.get('x_concepts', [])[:5]] # Top 5 concepts
        }
        return author_details
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from OpenAlex API: {e}", file=sys.stderr)
        return None

def search_theses_fr(author_name):
    """
    Searches theses.fr for a given author name and returns the search results.
    
    Args:
        author_name (str): The full name of the author to search for.
        
    Returns:
        list: A list of dictionaries, each representing a found thesis.
              Returns an empty list if no results or an error occurs.
    """
    search_url = "https://theses.fr/api/v1/personnes/recherche/"
    params = {'q': author_name, 'debut': 0, 'nombre': 10, 'tri': 'pertinence'}
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        print(f"Searching theses.fr API for '{author_name}'...")
        response = requests.get(search_url, params=params, headers=headers, verify=False)
        print(f"--> Requesting URL: {response.url}")
        response.raise_for_status()
        
        data = response.json()
        
        if not data.get('personnes'):
            return []

        results = []
        for person in data['personnes']:
            # We only care about theses where the person is the author.
            if 'Auteur / Autrice' in person.get('roles', {}):
                thesis_info = {
                    'name': f"{person.get('prenom')} {person.get('nom')}",
                    'idref': person.get('id'),
                    'thesis_id': person.get('these'),
                    'disciplines': ", ".join(person.get('disciplines', [])),
                    'establishments': ", ".join(person.get('etablissements', []))
                }
                results.append(thesis_info)
            
        return results

    except requests.exceptions.RequestException as e:
        print(f"Error searching on theses.fr API: {e}", file=sys.stderr)
        return []
    except ValueError: # Catches JSON decoding errors
        print(f"Error decoding JSON from theses.fr API.", file=sys.stderr)
        return []

def find_best_match(author_details, thesis_records):
    """
    Scores and sorts thesis records based on how well they match the author's details.

    Args:
        author_details (dict): Details of the author from OpenAlex.
        thesis_records (list): A list of potential thesis records from theses.fr.

    Returns:
        list: A sorted list of thesis records with match scores and evidence.
    """
    scored_results = []

    for thesis in thesis_records:
        score = 0
        evidence = []

        # 1. Check for institution match (+10 points)
        if author_details['institution'] and thesis['establishments']:
            # Check if any part of the OpenAlex institution name is in the thesis establishments string
            if author_details['institution'].lower() in thesis['establishments'].lower():
                score += 10
                evidence.append(f"Institution match on '{author_details['institution']}'")

        # 2. Check for discipline/concept match (+5 points per match)
        if author_details['concepts'] and thesis['disciplines']:
            thesis_disciplines_lower = [d.lower() for d in thesis['disciplines'].split(', ')]
            for concept in author_details['concepts']:
                if concept in thesis_disciplines_lower:
                    score += 5
                    evidence.append(f"Discipline match on '{concept.title()}'")
        
        if score > 0:
            thesis['match_score'] = score
            thesis['match_evidence'] = evidence
            scored_results.append(thesis)

    # Sort by score in descending order
    return sorted(scored_results, key=lambda x: x['match_score'], reverse=True)

def main():
    parser = argparse.ArgumentParser(description="Verify an author's PhD from France using their OpenAlex ID.")
    parser.add_argument("author_id", help="The OpenAlex ID of the author to verify (e.g., A5000002327).")
    
    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)
        
    args = parser.parse_args()
    
    author_details = get_author_details(args.author_id)
    
    if not author_details or not author_details['display_name']:
        print(f"Could not retrieve details for author ID: {args.author_id}")
        sys.exit(1)
        
    print(f"Found author: {author_details['display_name']} (ID: {args.author_id})")
    if author_details['institution']:
        print(f"  Institution: {author_details['institution']}")
    if author_details['concepts']:
        print(f"  Concepts: {', '.join(author_details['concepts']).title()}")

    
    theses = search_theses_fr(author_details['display_name'])
    
    if not theses:
        print("\n--> No thesis records found on theses.fr for this author name.")
        sys.exit(0)

    best_matches = find_best_match(author_details, theses)

    if not best_matches:
        print("\n--> Found thesis records, but none could be confidently matched by institution or discipline.")
        print("    Displaying top raw result as a fallback:")
        thesis = theses[0]
        print("\n  Record 1:")
        print(f"    Name:           {thesis['name']}")
        print(f"    ID:             {thesis['idref']}")
        print(f"    Thesis ID:      {thesis['thesis_id']}")
        print(f"    Disciplines:    {thesis['disciplines']}")
        print(f"    Establishments: {thesis['establishments']}")

    else:
        print(f"\n--> Found {len(best_matches)} confident match(es). Best match presented first:")
        for i, thesis in enumerate(best_matches, 1):
            print(f"\n  Record {i} (Match Score: {thesis['match_score']}):")
            print(f"    Name:           {thesis['name']}")
            print(f"    ID:             {thesis['idref']}")
            print(f"    Thesis ID:      {thesis['thesis_id']}")
            print(f"    Disciplines:    {thesis['disciplines']}")
            print(f"    Establishments: {thesis['establishments']}")
            print(f"    Match Evidence: {', '.join(thesis['match_evidence'])}")

if __name__ == '__main__':
    main() 
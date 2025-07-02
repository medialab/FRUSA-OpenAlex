#!/usr/bin/env python3
"""
Enhanced PhD detection script using minet for fast parallel processing.

This script processes ALL authors to mark those who presumably did their PhD in France.
Uses minet for efficient HTTP requests with built-in retry and rate limiting.
Includes minet's native browser cookie extraction and user agent spoofing.
Features intelligent rate limiting to respect API limits (9 req/sec for OpenAlex).
"""

import pandas as pd
import json
import sys
import logging
from pathlib import Path
import argparse
import subprocess
import urllib.parse
import tempfile
import os
import time
import threading
from typing import Optional

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Global flag for browser authentication
USE_BROWSER_AUTH = True

class TokenBucketRateLimiter:
    """
    Token bucket rate limiter for API requests.
    Allows bursts up to bucket capacity while maintaining average rate.
    """
    
    def __init__(self, rate: float, capacity: Optional[float] = None):
        """
        Initialize the rate limiter.
        
        Args:
            rate: Tokens per second (e.g., 8.5 for 8.5 requests/second)
            capacity: Maximum tokens in bucket (defaults to rate)
        """
        self.rate = rate
        self.capacity = capacity or rate
        self.tokens = self.capacity
        self.last_update = time.time()
        self.lock = threading.Lock()
        
    def acquire(self, tokens: int = 1) -> bool:
        """
        Try to acquire tokens. Returns True if successful, False otherwise.
        """
        with self.lock:
            now = time.time()
            # Add tokens based on elapsed time
            elapsed = now - self.last_update
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            self.last_update = now
            
            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
            return False
    
    def wait_for_token(self, tokens: int = 1):
        """
        Wait until we can acquire the required tokens.
        """
        while not self.acquire(tokens):
            # Calculate how long to wait
            with self.lock:
                tokens_needed = tokens - self.tokens
                wait_time = tokens_needed / self.rate
            
            # Add a small buffer to avoid busy waiting
            time.sleep(min(wait_time + 0.01, 0.1))

# Global rate limiters for different APIs
OPENALEX_LIMITER = TokenBucketRateLimiter(rate=8.5, capacity=10)  # 8.5 req/sec, allow small bursts
THESES_LIMITER = TokenBucketRateLimiter(rate=5.0, capacity=8)     # 5 req/sec for theses.fr

def prepare_author_urls(input_file: str, output_file: str, filter_france: bool = False) -> int:
    """Prepare OpenAlex author URLs for minet processing."""
    try:
        logger.info(f"Reading author data from {input_file}")
        df = pd.read_csv(input_file)
        
        authors_to_process = []
        france_count = 0
        
        for _, row in df.iterrows():
            author_id = row['author_id']
            country_seq = row.get('country_codes_sequence', '')
            
            # Compute flags
            started_in_france = False
            ever_in_france = False
            if isinstance(country_seq, str) and country_seq:
                # Normalize sequence and check for FR anywhere
                if 'FR' in country_seq.split(',') or 'FR' in country_seq:
                    ever_in_france = True
                
                countries = [c.strip() for c in country_seq.split(' -> ')]
                first_country = None
                for country in countries:
                    if country and country.lower() not in ['empty', '<empty>']:
                        first_country = country.split(',')[0].strip()
                        break
                if first_country == 'FR':
                    started_in_france = True
                    france_count += 1
            
            # Add to processing list
            if not filter_france or started_in_france:
                authors_to_process.append({
                    'author_id': author_id,
                    'url': f"https://api.openalex.org/authors/{author_id}",
                    'started_in_france': started_in_france,
                    'ever_in_france': ever_in_france,
                    'country_sequence': country_seq
                })
        
        logger.info(f"Authors starting in France: {france_count}")
        logger.info(f"Total to process: {len(authors_to_process)}")
        
        pd.DataFrame(authors_to_process).to_csv(output_file, index=False)
        return len(authors_to_process)
        
    except Exception as e:
        logger.error(f"Error preparing URLs: {e}")
        return 0

def fetch_with_minet(urls_file: str, output_file: str, rate_limit: int = 50) -> bool:
    """Use minet to fetch URLs in parallel with rate limiting for OpenAlex."""
    try:
        # Calculate throttle to stay under 8.5 req/sec with safety margin
        # With 8 threads, each thread should wait ~1.0 second between requests
        throttle_time = 1.0  # 1 second between requests per thread
        
        cmd = [
            'minet', 'fetch', 'url',
            '-i', urls_file,
            '-o', output_file,
            '--throttle', str(throttle_time),  # 1 second throttle for safety
            '--threads', '8',  # Reduced threads to respect rate limits
            '--domain-parallelism', '1',  # Must be 1 when using throttle
            '--timeout', '30',
            '--retries', '3'
        ]
        
        # Add browser authentication if enabled
        if USE_BROWSER_AUTH:
            cmd.extend([
                '-g', 'firefox',  # Grab cookies from Firefox
                '--spoof-user-agent'  # Use realistic user agent
            ])
        else:
            # Even without cookies, spoof user agent to appear like a real browser
            cmd.append('--spoof-user-agent')
        
        auth_status = "with browser auth" if USE_BROWSER_AUTH else "with user agent spoofing only"
        logger.info(f"Running minet fetch {auth_status} (8.5 req/sec limit): {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            logger.info("Minet fetch completed successfully")
            return True
        else:
            logger.error(f"Minet failed: {result.stderr}")
            return False
            
    except Exception as e:
        logger.error(f"Error running minet: {e}")
        return False

def fetch_with_minet_custom(urls_file: str, output_file: str, threads: int, domain_parallelism: int, throttle: float) -> bool:
    """Use minet to fetch URLs in parallel with custom performance settings and rate limiting."""
    try:
        # Override aggressive settings to respect rate limits
        if throttle == 0.0:
            # If no throttle specified, calculate safe throttle for OpenAlex
            safe_throttle = max(0.12, 1.0 / 8.5)  # At least 0.12s between requests (8.5 req/sec)
            logger.info(f"Auto-adjusting throttle from 0 to {safe_throttle:.2f}s to respect rate limits")
            throttle = safe_throttle
        
        # Limit threads to avoid overwhelming APIs
        if threads > 8:
            logger.info(f"Reducing threads from {threads} to 8 to respect rate limits")
            threads = 8
        
        # If throttle > 0, we must set domain-parallelism to 1 due to minet restrictions
        actual_domain_parallelism = 1 if throttle > 0 else min(domain_parallelism, 1)
        
        cmd = [
            'minet', 'fetch', 'url',
            '-i', urls_file,
            '-o', output_file,
            '--throttle', str(throttle),
            '--threads', str(threads),
            '--domain-parallelism', str(actual_domain_parallelism),
            '--timeout', '30',
            '--retries', '3'
        ]
        
        # Add browser authentication if enabled
        if USE_BROWSER_AUTH:
            cmd.extend([
                '-g', 'firefox',  # Grab cookies from Firefox
                '--spoof-user-agent'  # Use realistic user agent
            ])
        else:
            # Even without cookies, spoof user agent to appear like a real browser
            cmd.append('--spoof-user-agent')
        
        logger.info(f"Using throttle {throttle}s with {threads} threads (respects 8.5 req/sec limit)")
        
        auth_status = "with browser auth" if USE_BROWSER_AUTH else "with user agent spoofing only"
        logger.info(f"Running minet fetch {auth_status}: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            logger.info("Minet fetch completed successfully")
            return True
        else:
            logger.error(f"Minet failed: {result.stderr}")
            return False
            
    except Exception as e:
        logger.error(f"Error running minet: {e}")
        return False

def fetch_thesis_with_minet(urls_file: str, output_file: str) -> bool:
    """Use minet to fetch theses.fr URLs with conservative rate limiting."""
    try:
        # Conservative settings for theses.fr
        throttle_time = 0.2  # 5 req/sec max
        
        cmd = [
            'minet', 'fetch', 'url',
            '-i', urls_file,
            '-o', output_file,
            '--throttle', str(throttle_time),  # Conservative throttle
            '--threads', '5',  # Reduced threads for theses.fr
            '--domain-parallelism', '1',  # Must be 1 when using throttle
            '--timeout', '30',
            '--retries', '3',
            '--insecure'  # theses.fr has SSL issues
        ]
        
        # Add browser authentication if enabled
        if USE_BROWSER_AUTH:
            cmd.extend([
                '-g', 'firefox',  # Grab cookies from Firefox
                '--spoof-user-agent'  # Use realistic user agent
            ])
        else:
            # Even without cookies, spoof user agent to appear like a real browser
            cmd.append('--spoof-user-agent')
        
        auth_status = "with browser auth" if USE_BROWSER_AUTH else "with user agent spoofing only"
        logger.info(f"Running minet thesis search {auth_status} (5 req/sec limit): {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            logger.info("Minet thesis search completed successfully")
            return True
        else:
            logger.error(f"Minet thesis search failed: {result.stderr}")
            return False
            
    except Exception as e:
        logger.error(f"Error running minet thesis search: {e}")
        return False

def fetch_thesis_with_minet_custom(urls_file: str, output_file: str, threads: int, domain_parallelism: int, throttle: float) -> bool:
    """Use minet to fetch theses.fr URLs with custom performance settings and rate limiting."""
    try:
        # Override aggressive settings for theses.fr
        if throttle == 0.0:
            # Conservative throttle for theses.fr
            safe_throttle = 0.2  # 5 req/sec
            logger.info(f"Auto-adjusting throttle from 0 to {safe_throttle}s for theses.fr")
            throttle = safe_throttle
        
        # Limit threads for theses.fr
        if threads > 5:
            logger.info(f"Reducing threads from {threads} to 5 for theses.fr")
            threads = 5
        
        # If throttle > 0, we must set domain-parallelism to 1 due to minet restrictions
        actual_domain_parallelism = 1 if throttle > 0 else min(domain_parallelism, 1)
        
        cmd = [
            'minet', 'fetch', 'url',
            '-i', urls_file,
            '-o', output_file,
            '--throttle', str(throttle),
            '--threads', str(threads),
            '--domain-parallelism', str(actual_domain_parallelism),
            '--timeout', '30',
            '--retries', '3',
            '--insecure'  # theses.fr has SSL issues
        ]
        
        # Add browser authentication if enabled
        if USE_BROWSER_AUTH:
            cmd.extend([
                '-g', 'firefox',  # Grab cookies from Firefox
                '--spoof-user-agent'  # Use realistic user agent
            ])
        else:
            # Even without cookies, spoof user agent to appear like a real browser
            cmd.append('--spoof-user-agent')
        
        logger.info(f"Using throttle {throttle}s with {threads} threads for theses.fr")
        
        auth_status = "with browser auth" if USE_BROWSER_AUTH else "with user agent spoofing only"
        logger.info(f"Running minet thesis search {auth_status}: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            logger.info("Minet thesis search completed successfully")
            return True
        else:
            logger.error(f"Minet thesis search failed: {result.stderr}")
            return False
            
    except Exception as e:
        logger.error(f"Error running minet thesis search: {e}")
        return False

def extract_author_names(minet_output: str, names_output: str) -> int:
    """Extract author names from minet results."""
    try:
        df = pd.read_csv(minet_output)
        successful = df[df['http_status'] == 200].copy()  # Use http_status instead of status
        
        author_names = []
        for _, row in successful.iterrows():
            try:
                # Read from downloaded JSON file
                json_path = Path('downloaded') / row['path']
                if json_path.exists():
                    with open(json_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    name = data.get('display_name')
                    if name:
                        author_names.append({
                            'author_id': row['author_id'],
                            'display_name': name,
                            'started_in_france': row['started_in_france'],
                            'ever_in_france': row.get('ever_in_france', False),
                            'country_sequence': row['country_sequence']
                        })
            except (json.JSONDecodeError, KeyError, FileNotFoundError):
                continue
        
        pd.DataFrame(author_names).to_csv(names_output, index=False)
        logger.info(f"Extracted {len(author_names)} names")
        return len(author_names)
        
    except Exception as e:
        logger.error(f"Error extracting names: {e}")
        return 0

def prepare_thesis_urls(names_file: str, urls_output: str) -> int:
    """Prepare theses.fr search URLs."""
    try:
        df = pd.read_csv(names_file)
        search_urls = []
        
        for _, row in df.iterrows():
            name = row['display_name']
            if name:
                encoded_name = urllib.parse.quote_plus(name)
                url = f"https://theses.fr/api/v1/personnes/recherche/?q={encoded_name}"
                search_urls.append({
                    'author_id': row['author_id'],
                    'display_name': name,
                    'url': url,
                    'started_in_france': row['started_in_france'],
                    'ever_in_france': row.get('ever_in_france', False),
                    'country_sequence': row['country_sequence']
                })
        
        pd.DataFrame(search_urls).to_csv(urls_output, index=False)
        logger.info(f"Prepared {len(search_urls)} thesis search URLs")
        return len(search_urls)
        
    except Exception as e:
        logger.error(f"Error preparing thesis URLs: {e}")
        return 0

def analyze_thesis_results(results_file: str, final_output: str) -> dict:
    """Analyze thesis search results and create final classification."""
    try:
        df = pd.read_csv(results_file)
        
        stats = {
            'total': len(df),
            'successful_searches': 0,
            'potential_matches': 0,
            'confident_matches': 0,
            'france_starters_with_phd': 0,
            'authors_ever_in_france': 0
        }
        
        final_results = []
        
        for _, row in df.iterrows():
            result = {
                'author_id': row['author_id'],
                'display_name': row['display_name'],
                'started_in_france': row['started_in_france'],
                'ever_in_france': row.get('ever_in_france', False),
                'country_sequence': row['country_sequence'],
                'has_potential_thesis': False,
                'thesis_confidence': 'none',
                'thesis_details': None
            }
            
            # Update stats for authors ever in France
            if result['ever_in_france']:
                stats['authors_ever_in_france'] += 1
            
            if row['http_status'] == 200:  # Use http_status instead of status
                stats['successful_searches'] += 1
                try:
                    # Read from downloaded JSON file
                    json_path = Path('downloaded') / row['path']
                    if json_path.exists():
                        with open(json_path, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                        
                        personnes = data.get('personnes', [])
                        
                        if personnes:
                            stats['potential_matches'] += 1
                            result['has_potential_thesis'] = True
                            
                            # Check for author role (high confidence)
                            for person in personnes:
                                if 'Auteur / Autrice' in person.get('roles', {}):
                                    stats['confident_matches'] += 1
                                    result['thesis_confidence'] = 'high'
                                    result['thesis_details'] = json.dumps({
                                        'name': f"{person.get('prenom', '')} {person.get('nom', '')}".strip(),
                                        'idref': person.get('id'),
                                        'thesis_id': person.get('these'),
                                        'disciplines': person.get('disciplines', []),
                                        'establishments': person.get('etablissements', [])
                                    })
                                    break
                            else:
                                result['thesis_confidence'] = 'medium'
                            
                            if row['started_in_france']:
                                stats['france_starters_with_phd'] += 1
                                
                except (json.JSONDecodeError, KeyError, FileNotFoundError):
                    pass
            
            final_results.append(result)
        
        # Save results
        pd.DataFrame(final_results).to_csv(final_output, index=False)
        
        # Log stats
        logger.info("=== Results Summary ===")
        for key, value in stats.items():
            logger.info(f"{key}: {value}")
            
        return stats
        
    except Exception as e:
        logger.error(f"Error analyzing results: {e}")
        return {}

def main():
    global USE_BROWSER_AUTH
    
    parser = argparse.ArgumentParser(description="Mark authors with French PhD evidence using minet")
    parser.add_argument('input_file', help='Input CSV with author sequences')
    parser.add_argument('--output', '-o', default='authors_with_phd_marks.csv', 
                       help='Output CSV file')
    parser.add_argument('--france-only', '-f', action='store_true',
                       help='Only process France starters')
    parser.add_argument('--keep-temp', action='store_true',
                       help='Keep intermediate files')
    
    # Performance tuning arguments
    parser.add_argument('--openalex-threads', type=int, default=8,
                       help='Number of threads for OpenAlex requests (default: 8)')
    parser.add_argument('--openalex-domain-parallelism', type=int, default=1,
                       help='Domain parallelism for OpenAlex requests (default: 1)')
    parser.add_argument('--thesis-threads', type=int, default=5,
                       help='Number of threads for theses.fr requests (default: 5)')
    parser.add_argument('--thesis-domain-parallelism', type=int, default=1,
                       help='Domain parallelism for theses.fr requests (default: 1)')
    parser.add_argument('--throttle', type=float, default=0.12,
                       help='Throttle time between requests in seconds (default: 0.12 for 8.5 req/sec)')
    parser.add_argument('--no-cookies', action='store_true',
                       help='Disable browser cookie extraction (use if browser access fails)')
    
    args = parser.parse_args()
    
    # Set global cookie flag
    USE_BROWSER_AUTH = not args.no_cookies
    
    # File names
    base = Path(args.input_file).stem
    author_urls = f"{base}_urls.csv"
    openalex_results = f"{base}_openalex.csv"
    names_file = f"{base}_names.csv"
    thesis_urls = f"{base}_thesis_urls.csv"
    thesis_results = f"{base}_thesis_results.csv"
    
    logger.info("Starting PhD detection pipeline with minet")
    cookie_status = "enabled" if USE_BROWSER_AUTH else "disabled"
    logger.info(f"Performance settings: OpenAlex({args.openalex_threads} threads, {args.openalex_domain_parallelism} domain parallelism), "
               f"Theses.fr({args.thesis_threads} threads, {args.thesis_domain_parallelism} domain parallelism), "
               f"Throttle: {args.throttle}s, Browser cookies: {cookie_status}")
    
    # Step 1: Prepare OpenAlex URLs
    if not prepare_author_urls(args.input_file, author_urls, args.france_only):
        return 1
    
    # Step 2: Fetch author data
    logger.info("Fetching author names from OpenAlex...")
    if not fetch_with_minet_custom(author_urls, openalex_results, 
                                  args.openalex_threads, args.openalex_domain_parallelism, args.throttle):
        return 1
    
    # Step 3: Extract names
    if not extract_author_names(openalex_results, names_file):
        return 1
    
    # Step 4: Prepare thesis search URLs
    if not prepare_thesis_urls(names_file, thesis_urls):
        return 1
    
    # Step 5: Search theses.fr
    logger.info("Searching theses.fr...")
    if not fetch_thesis_with_minet_custom(thesis_urls, thesis_results,
                                         args.thesis_threads, args.thesis_domain_parallelism, args.throttle):
        return 1
    
    # Step 6: Analyze and save final results
    if not analyze_thesis_results(thesis_results, args.output):
        return 1
    
    logger.info(f"PhD detection completed! Results in {args.output}")
    
    # Cleanup
    if not args.keep_temp:
        for temp_file in [author_urls, openalex_results, names_file, thesis_urls, thesis_results]:
            try:
                Path(temp_file).unlink()
            except FileNotFoundError:
                pass
    
    return 0

if __name__ == '__main__':
    sys.exit(main()) 
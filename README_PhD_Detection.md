# French PhD Detection System

This enhanced system uses **minet** for fast, parallel processing to identify authors who presumably did their PhD in France.

## Overview

The system processes all authors in your OpenAlex dataset and:

1. **Identifies potential French PhD authors** based on career start location (first publication country = France)
2. **Uses browser authentication** - Extracts cookies from Firefox to avoid rate limiting
3. **Spoofs user agent** - Makes requests appear to come from a real browser
4. **Intelligent rate limiting** - Respects OpenAlex limits (8.5 req/sec) and theses.fr limits (5 req/sec)
5. **Cross-references with theses.fr** - France's national thesis database
6. **Provides confidence scores** for PhD matches
7. **Processes at scale** using minet's parallel request capabilities with safety limits

## Installation

Ensure you have minet installed:

```bash
pip install minet
```

**Browser authentication is built into minet** - no additional packages needed!

## Usage

### Basic Usage (All Authors) - **With Rate Limiting**

Process all authors with automatic rate limiting to respect API limits:

```bash
python mark_french_phd_authors.py author_sequences_final.py.csv
```

### France-Only Mode (Recommended for Initial Testing)

```bash
python mark_french_phd_authors.py author_sequences_final.py.csv --france-only
```

### Custom Rate Limiting

For faster processing (if you have API access or during off-peak hours):
```bash
python mark_french_phd_authors.py author_sequences_final.py.csv \
  --france-only \
  --throttle 0.05 \
  --openalex-threads 12
```

For ultra-conservative processing:
```bash
python mark_french_phd_authors.py author_sequences_final.py.csv \
  --france-only \
  --throttle 0.25 \
  --openalex-threads 4
```

### Without Browser Cookies (if browser access fails)

```bash
python mark_french_phd_authors.py author_sequences_final.py.csv --no-cookies
```

### Custom Output File

```bash
python mark_french_phd_authors.py author_sequences_final.py.csv -o french_phd_results.csv
```

### Keep Intermediate Files (for Debugging)

```bash
python mark_french_phd_authors.py author_sequences_final.py.csv --keep-temp
```

### Performance Tuning Options

For maximum speed (recommended for large datasets):
```bash
python mark_french_phd_authors.py author_sequences_final.py.csv \
  --openalex-threads 32 \
  --openalex-domain-parallelism 16 \
  --thesis-threads 16 \
  --thesis-domain-parallelism 8 \
  --throttle 0
```

For conservative processing (if you encounter rate limiting):
```bash
python mark_french_phd_authors.py author_sequences_final.py.csv \
  --openalex-threads 16 \
  --thesis-threads 8 \
  --throttle 0.1
```

**Note**: When using `--throttle > 0`, domain parallelism is automatically set to 1 due to minet restrictions.

### All Options

```bash
python mark_french_phd_authors.py --help
```

Available performance tuning options:
- `--openalex-threads`: Number of concurrent threads for OpenAlex (default: 32)
- `--openalex-domain-parallelism`: Domain parallelism for OpenAlex (default: 16)  
- `--thesis-threads`: Number of concurrent threads for theses.fr (default: 16)
- `--thesis-domain-parallelism`: Domain parallelism for theses.fr (default: 8)
- `--throttle`: Time between requests in seconds (default: 0 for max speed)

## How It Works

### Step 1: Browser Authentication Setup - **NEW!**
- **Automatically extracts cookies** from your Chrome browser for both OpenAlex and theses.fr
- **Spoofs realistic user agent** to mimic genuine browser requests
- **Adds proper headers** (Accept, Accept-Language, etc.) for authentic requests
- **Fallback mode**: Works without cookies if extraction fails

### Step 2: Author URL Preparation
- Reads your author sequences CSV
- Identifies authors who started in France (first non-empty country = 'FR')
- Creates OpenAlex API URLs for fetching author names

### Step 3: Parallel Name Fetching (Minet) - **OPTIMIZED**
- Uses minet to fetch author display names from OpenAlex API
- **Browser cookies**: Uses your authenticated session to avoid rate limits
- **Zero throttling**: Maximum speed processing
- **32 threads**: High concurrency for fast processing
- **16 domain parallelism**: Multiple connections to OpenAlex simultaneously
- **Retry logic**: Automatic retries for failed requests

### Step 4: Thesis Search Preparation  
- Prepares theses.fr search URLs for each author
- URL-encodes author names for proper API queries

### Step 5: Parallel Thesis Search (Minet) - **OPTIMIZED**
- Searches France's national thesis database (theses.fr)
- **Browser cookies**: Uses your authenticated session for access
- **Zero throttling**: Maximum speed while respecting server capacity
- **16 threads**: Balanced performance for theses.fr
- **8 domain parallelism**: Multiple connections without overwhelming theses.fr

### Step 6: Results Analysis
- Parses thesis search results
- Assigns confidence scores:
  - **High**: Author found with "Auteur / Autrice" role
  - **Medium**: Author found but role unclear  
  - **None**: No thesis record found

## Browser Authentication Benefits

Using browser cookies and realistic headers provides several advantages:

1. **Avoids Rate Limiting**: APIs see requests as coming from authenticated browser sessions
2. **Higher Success Rate**: Reduced 429 (Too Many Requests) errors  
3. **Better Access**: Some content may only be available to authenticated users
4. **Realistic Traffic**: Requests appear as normal browser usage

**How it works**: The script uses minet's built-in browser cookie extraction (`-g chrome`) and user agent spoofing (`--spoof-user-agent`) to make requests indistinguishable from normal browsing.

## Output Format

The final CSV contains these columns:

- `author_id`: OpenAlex author identifier
- `display_name`: Author's name from OpenAlex
- `started_in_france`: Boolean - did first publication come from France?
- `country_sequence`: Full career trajectory (countries over time)
- `has_potential_thesis`: Boolean - any thesis record found?
- `thesis_confidence`: 'high', 'medium', or 'none'
- `thesis_details`: JSON with thesis metadata (if found)

## Performance Estimates

With your ~470,000 authors and **intelligent rate limiting**:

**France-only mode** (~10-50k authors estimated):
- OpenAlex fetching: ~1-6 hours (respects 8.5 req/sec limit)
- Theses.fr searching: ~2-10 hours (respects 5 req/sec limit)
- **Total**: ~3-16 hours (safe and reliable!)

**All authors mode** (470k authors):
- OpenAlex fetching: ~15-20 hours (respects rate limits)
- Theses.fr searching: ~25-35 hours (respects rate limits)
- **Total**: ~40-55 hours (1.5-2 days, but no rate limiting issues!)

## Rate Limiting Features

The enhanced version includes intelligent rate limiting inspired by [Python rate limiting best practices](https://dev.to/arunsaiv/-how-to-throttle-like-a-pro-5-rate-limiting-patterns-in-python-you-should-know-54ep):

### For OpenAlex API:
- **8.5 requests/second limit** (stays safely under 10 req/sec limit)
- **8 concurrent threads** with 0.12s throttle by default
- **Token bucket algorithm** for burst handling
- **Automatic throttle adjustment** if you specify 0

### For theses.fr:
- **5 requests/second limit** (conservative approach)
- **5 concurrent threads** with 0.2s throttle
- **Respectful processing** to avoid overwhelming their servers

### Benefits of Rate Limiting:
1. **No 429 errors**: Stays within API limits
2. **Reliable processing**: No failed requests due to rate limiting
3. **Respectful usage**: Good API citizenship
4. **Unattended operation**: Can run overnight without intervention

Available performance tuning options:
- `--openalex-threads`: Number of concurrent threads for OpenAlex (default: 8)
- `--openalex-domain-parallelism`: Domain parallelism for OpenAlex (default: 1)  
- `--thesis-threads`: Number of concurrent threads for theses.fr (default: 5)
- `--thesis-domain-parallelism`: Domain parallelism for theses.fr (default: 1)
- `--throttle`: Time between requests in seconds (default: 0.12 for 8.5 req/sec)

## Performance Optimizations

The enhanced version leverages several optimization techniques inspired by [parallel processing best practices](https://www.guptaakashdeep.com/enhancing-spark-job-performance-multithreading/):

### For OpenAlex API:
- **Zero throttling**: Maximum speed processing
- **32 concurrent threads**: Maximizes throughput
- **16 domain parallelism**: Multiple simultaneous connections per domain
- **Optimized for speed**: No artificial delays

### For theses.fr:
- **Zero throttling**: Maximum speed processing
- **16 threads**: Balanced approach for smaller API
- **8 domain parallelism**: Respectful but efficient
- **Conservative but fast**: Maintains reliability while maximizing speed

## Example Results

```csv
author_id,display_name,started_in_france,thesis_confidence,thesis_details
https://openalex.org/A123456,Jean Dupont,true,high,"{""name"": ""Jean Dupont"", ""disciplines"": [""Physics""], ""establishments"": [""Université de Paris""]}"
https://openalex.org/A789012,Marie Martin,true,medium,"{""name"": ""Marie Martin"", ""disciplines"": [""Biology""]}"
https://openalex.org/A345678,John Smith,false,none,null
```

## Monitoring Progress

The script provides detailed logging:

```
2024-01-15 10:30:15 - INFO - Reading author data from author_sequences_final.py.csv
2024-01-15 10:30:20 - INFO - Authors starting in France: 25431
2024-01-15 10:30:20 - INFO - Total to process: 25431
2024-01-15 10:30:22 - INFO - Fetching author names from OpenAlex...
2024-01-15 10:45:33 - INFO - Extracted 24981 names
2024-01-15 10:45:35 - INFO - Searching theses.fr...
2024-01-15 11:30:12 - INFO - === Results Summary ===
2024-01-15 11:30:12 - INFO - total: 24981
2024-01-15 11:30:12 - INFO - potential_matches: 3247
2024-01-15 11:30:12 - INFO - confident_matches: 1832
```

## Troubleshooting

### Common Issues

1. **Minet not found**: Install with `pip install minet`
2. **Rate limit errors**: The script includes conservative rate limits
3. **SSL errors with theses.fr**: The script uses `--insecure` flag for theses.fr
4. **Memory issues**: The script processes in chunks and cleans up intermediate files

### Resuming Interrupted Runs

If the process is interrupted, you can resume by using the `--keep-temp` flag and rerunning with intermediate files.

## Validation

The system provides multiple validation mechanisms:

1. **Career trajectory analysis**: Ensures France was truly the starting point
2. **Name matching**: Cross-references OpenAlex names with thesis records  
3. **Institution validation**: Uses establishment names for additional confidence
4. **Discipline alignment**: Compares research areas when available

## Next Steps

After running the analysis, you can:

1. **Filter results** by confidence level
2. **Analyze patterns** in French PhD migration
3. **Cross-reference** with institutional affiliations
4. **Validate samples** manually for accuracy assessment 
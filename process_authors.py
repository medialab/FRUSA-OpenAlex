import pandas as pd
import sys
from collections import defaultdict

def create_author_sequences_optimized(input_file, output_file):
    """
    Reads a large author sequences CSV in chunks, processes it efficiently to
    create publication and country sequences for each author, and saves the
    result to a new CSV.

    Args:
        input_file (str): Path to the input CSV file.
        output_file (str): Path to save the output CSV file.
    """
    try:
        # This dictionary will store {author_id: [(year, country_codes), ...]}
        # It's much more memory-efficient than a massive DataFrame.
        author_data = defaultdict(list)

        # Define dtypes and columns for efficiency. We only load what we need.
        use_cols = ['author_id', 'publication_year', 'country_codes']
        
        # Process the file in chunks of 1 million rows.
        # Adjust this size based on your server's memory.
        chunk_size = 1_000_000
        
        print(f"Reading input file '{input_file}' in chunks of {chunk_size:,} rows...")

        chunk_iterator = pd.read_csv(
            input_file,
            header=0,  # Treat the first row as a header.
            usecols=use_cols,
            on_bad_lines='skip',
            engine='c',  # Using the much faster C engine
            sep=',',
            chunksize=chunk_size,
            dtype=str  # Read all used columns as strings to be safe.
        )
        
        total_rows_processed = 0
        for i, chunk in enumerate(chunk_iterator):
            # --- Clean data in the chunk ---
            # Robustly convert year to a number, discarding rows that fail.
            chunk['publication_year'] = pd.to_numeric(chunk['publication_year'], errors='coerce')
            chunk.dropna(subset=['author_id', 'publication_year'], inplace=True)
            chunk['publication_year'] = chunk['publication_year'].astype('int32')
            
            # Clean country codes
            chunk['country_codes'] = chunk['country_codes'].fillna('<empty>').str.replace('[{}]', '', regex=True)

            # --- Aggregate data into the dictionary ---
            for row in chunk.itertuples(index=False):
                author_data[row.author_id].append((row.publication_year, row.country_codes))
            
            total_rows_processed += len(chunk)
            print(f"  ...processed chunk {i + 1}, total rows so far: {total_rows_processed:,}")

        print("\nAll chunks processed. Aggregating sequences for each author...")

        # --- Convert aggregated data to final format ---
        final_results = []
        for author_id, records in author_data.items():
            records.sort(key=lambda x: x[0])  # Sort by publication year
            
            pub_year_seq = ' -> '.join(str(r[0]) for r in records)
            country_seq = ' -> '.join(r[1] for r in records)
            
            final_results.append({
                'author_id': author_id,
                'publication_years_sequence': pub_year_seq,
                'country_codes_sequence': country_seq
            })

        print(f"Aggregation complete. Creating final DataFrame for {len(final_results):,} authors.")
        output_df = pd.DataFrame(final_results)
        
        print(f"Saving results to '{output_file}'...")
        output_df.to_csv(output_file, index=False)
        
        print("\nProcessing complete!")

    except FileNotFoundError:
        print(f"Error: Input file not found at '{input_file}'", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    INPUT_CSV = 'author_sequences2.csv'
    OUTPUT_CSV = 'author_sequences_final.py.csv'
    create_author_sequences_optimized(INPUT_CSV, OUTPUT_CSV) 
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import sys

def classify_career(sequence):
    """
    Classifies an author's career trajectory based on their country sequence.

    Args:
        sequence (str): A ' -> ' separated string of country codes.

    Returns:
        str: The career path category.
    """
    if not isinstance(sequence, str):
        return 'Invalid Sequence'

    countries = [c.strip() for c in sequence.split('->')]
    
    # This function assumes the first country is 'FR', as filtering is done prior.
    is_always_france = all(c == 'FR' for c in countries)
    if is_always_france:
        return 'Stayed in France'

    # If not always France, they must have left at some point.
    last_location_raw = countries[-1]
    last_locations = set(c.strip() for c in last_location_raw.split(','))
    
    journey_had_us = any('US' in c.split(',') for c in countries)
    
    is_last_only_france = (last_location_raw == 'FR')

    if is_last_only_france:
        if journey_had_us:
            return 'Round-trip (via US)'
        else:
            return 'Round-trip (no US)'
    
    # At this point, they did not return to only-France
    is_last_in_us = 'US' in last_locations
    is_last_in_france_too = 'FR' in last_locations

    if is_last_in_us and not is_last_in_france_too:
        return 'Expatriate (to US)'
    
    if not is_last_in_us and not is_last_in_france_too:
        return 'Expatriate (abroad, no US)'

    # All other cases fall here: e.g., last is 'FR,US' or 'FR,DE'
    return 'Complex/Ongoing Migration'

def create_migration_visual(input_file, output_image):
    """
    Reads author sequences, filters for French careers, classifies trajectories,
    and generates a year-by-year visualization of cohort trends.

    Args:
        input_file (str): Path to the input CSV file.
        output_image (str): Path to save the output plot image.
    """
    try:
        print(f"Reading data from '{input_file}'...")
        df = pd.read_csv(input_file)
    except FileNotFoundError:
        print(f"Error: The input file '{input_file}' was not found.", file=sys.stderr)
        print("Please run the `process_authors.py` script first to generate it.", file=sys.stderr)
        sys.exit(1)

    print("Filtering for authors starting in France post-1990...")
    
    # FIX: The 'min_year' column must be derived from the sequence string,
    # as it does not exist in the input CSV.
    df.dropna(subset=['publication_years_sequence', 'country_codes_sequence'], inplace=True)
    df['min_year'] = pd.to_numeric(df['publication_years_sequence'].str.split(' -> ').str[0], errors='coerce')

    # Now we can safely drop rows where min_year could not be parsed.
    df.dropna(subset=['min_year'], inplace=True)
    df['min_year'] = df['min_year'].astype(int)

    df_post_1990 = df[df['min_year'] > 1990].copy()

    df_post_1990['first_country'] = df_post_1990['country_codes_sequence'].str.split(' -> ').str[0]
    fr_starters = df_post_1990[df_post_1990['first_country'] == 'FR'].copy()
    
    if fr_starters.empty:
        print("No authors found matching the criteria (starting in France after 1990).", file=sys.stderr)
        sys.exit(0)

    print(f"Found {len(fr_starters)} authors. Classifying career trajectories per cohort year...")
    fr_starters['category'] = fr_starters['country_codes_sequence'].apply(classify_career)

    # Create a year-by-year breakdown of categories
    cohort_trends = fr_starters.groupby('min_year')['category'].value_counts().unstack(fill_value=0)
    
    # Calculate the percentage for a 100% stacked area chart
    cohort_percentages = cohort_trends.div(cohort_trends.sum(axis=1), axis=0) * 100
    
    # --- Visualization ---
    # FIX: Explicitly define all categories and colors to ensure none are dropped
    # and the stacking order is logical and consistent.
    
    all_categories = [
        'Stayed in France',
        'Round-trip (via US)',
        'Round-trip (no US)',
        'Expatriate (to US)',
        'Expatriate (abroad, no US)',
        'Complex/Ongoing Migration'
    ]
    
    colors = [
        '#440154',  # Dark Purple for 'Stayed'
        '#31688e',  # Blue for 'Returned via US'
        '#35b779',  # Green for 'Returned no US'
        '#fde725',  # Yellow for 'Expat to US'
        '#443a83',  # Indigo for 'Expat abroad'
        '#21908d'   # Teal for 'Complex'
    ]
    
    # Reindex the DataFrame to include all categories, filling missing ones with 0
    cohort_percentages = cohort_percentages.reindex(columns=all_categories, fill_value=0)

    print("Generating year-over-year visualization...")
    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(18, 10))
    
    # Plotting the stacked area chart with specified colors
    cohort_percentages.plot(
        kind='area', 
        stacked=True, 
        ax=ax, 
        linewidth=0.5,
        color=colors
    )
    
    # Set plot titles and labels
    ax.set_title('Yearly Cohort Trends of Researchers Starting in France (Post-1990)', fontsize=20, pad=20)
    ax.set_xlabel('Starting Year of Career', fontsize=14)
    ax.set_ylabel('Percentage of Authors (%)', fontsize=14)
    ax.tick_params(axis='x', labelsize=12, rotation=45)
    ax.tick_params(axis='y', labelsize=12)
    
    # Y-axis formatting
    ax.set_ylim(0, 100)
    ax.yaxis.set_major_formatter(plt.FuncFormatter('{:.0f}%'.format))
    
    # Legend formatting is now implicitly handled correctly by the ordered columns
    ax.legend(title='Career Path', bbox_to_anchor=(1.02, 1), loc='upper left')

    # Add grid and layout adjustments
    ax.grid(True, which='major', linestyle='--', linewidth='0.5', color='grey')
    fig.tight_layout()

    # Save the figure
    plt.savefig(output_image, dpi=300, bbox_inches='tight')
    print(f"Visualization saved to '{output_image}'")
    
    # Print final counts to console for the last 5 years as an example
    print("\n--- Career Path Breakdown (Counts for last 5 available years) ---")
    print(cohort_trends.tail(5))
    print("-----------------------------------------------------------------")


if __name__ == '__main__':
    # Define the input file from the previous script and the output for the plot
    INPUT_CSV = 'author_sequences_final.py.csv'
    OUTPUT_PNG = 'author_migration_from_france.png'
    create_migration_visual(INPUT_CSV, OUTPUT_PNG) 
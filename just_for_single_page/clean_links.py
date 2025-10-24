#!/usr/bin/env python3
"""
Clean links.csv by removing rows where href starts with '@https://www.marketing-mentor.com/ '
"""

import csv

def clean_links_csv(input_file='links.csv', output_file='links.csv'):
    """
    Remove rows from CSV where href column starts with 'https://www.marketing-mentor.com/ '
    
    Args:
        input_file (str): Input CSV file path
        output_file (str): Output CSV file path (can be same as input)
    """
    # Pattern to filter out
    pattern_to_remove = 'https://www.marketing-mentor.com/'
    
    # Read all rows
    rows_to_keep = []
    header = None
    removed_count = 0
    
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames
        
        for row in reader:
            href = row.get('href', '')
            
            # Keep row if href doesn't start with the pattern
            if not href.startswith(pattern_to_remove):
                rows_to_keep.append(row)
            else:
                removed_count += 1
                print(f"Removing: {row.get('text', 'N/A')} -> {href}")
    
    # Write cleaned data back
    with open(output_file, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows_to_keep)
    
    print(f"\n✓ Cleaned {input_file}")
    print(f"  Rows removed: {removed_count}")
    print(f"  Rows kept: {len(rows_to_keep)}")
    print(f"  Output saved to: {output_file}")


if __name__ == "__main__":
    clean_links_csv()


#!/usr/bin/env python3
"""
Label URLs using OpenAI GPT-4o mini API
Reads URLs from CSV and updates the isUseful column with True/False
"""

import csv
import os
import time
from openai import OpenAI
from dotenv import load_dotenv
from prompts.label_urls_prompt import label_urls_prompt


# Load environment variables from .env file
load_dotenv()


def label_urls_with_openai(input_csv, output_csv=None, api_key=None):
    """
    Label URLs using OpenAI API
    
    Args:
        input_csv (str): Input CSV file with URLs
        output_csv (str): Output CSV file (defaults to input_csv)
        api_key (str): OpenAI API key (or use OPENAI_API_KEY env var)
    """
    if output_csv is None:
        output_csv = input_csv
    
    # Initialize OpenAI client
    if api_key:
        client = OpenAI(api_key=api_key)
    else:
        # Will use OPENAI_API_KEY environment variable
        client = OpenAI()
    
    print("=" * 60)
    print("URL Labeling with OpenAI GPT-4o mini")
    print("=" * 60)
    print(f"Input file: {input_csv}")
    print(f"Output file: {output_csv}")
    print("-" * 60)
    
    # Read all rows from CSV and detect fieldnames
    rows = []
    fieldnames = []
    with open(input_csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames  # Preserve original fieldnames
        for row in reader:
            rows.append(row)
    
    total_urls = len(rows)
    print(f"Found {total_urls} URLs to label")
    print("-" * 60)
    
    # Track progress
    labeled_count = 0
    already_labeled = 0
    errors = 0
    
    try:
        for idx, row in enumerate(rows, 1):
            url = row['url']
            current_label = row.get('isUseful', '').strip()
            
            # Skip if already labeled
            if current_label:
                print(f"[{idx}/{total_urls}] Skipping (already labeled): {url}")
                already_labeled += 1
                continue
            
            print(f"[{idx}/{total_urls}] Labeling: {url}")
            
            try:
                # Create prompt with the URL
                prompt = label_urls_prompt.format(url=url)
                
                # Call OpenAI API
                response = client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {"role": "system", "content": "You are a precise labeling assistant. Respond only with 'True' or 'False'."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.2,
                    max_tokens=10
                )
                
                # Get the response
                label = response.choices[0].message.content.strip()
                
                # Validate response
                if label not in ['True', 'False']:
                    print(f"  ⚠ Unexpected response: {label}, defaulting to True")
                    label = 'True'
                
                # Update the row
                row['isUseful'] = label
                labeled_count += 1
                
                print(f"  ✓ Labeled as: {label}")
                
                # Write back to CSV after each label (real-time save)
                with open(output_csv, 'w', encoding='utf-8', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(rows)
                
                # Small delay to avoid rate limits
                time.sleep(0.5)
                
            except Exception as e:
                print(f"  ✗ Error: {e}")
                errors += 1
                # Set to True by default on error (as per "when unsure, choose True")
                row['isUseful'] = 'True'
                # Save even on error
                with open(output_csv, 'w', encoding='utf-8', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(rows)
    
    except KeyboardInterrupt:
        print("\n" + "-" * 60)
        print("Process interrupted by user!")
        print("Progress has been saved.")
    
    finally:
        # Final save
        with open(output_csv, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        
        print("-" * 60)
        print("Summary:")
        print(f"  Total URLs: {total_urls}")
        print(f"  Newly labeled: {labeled_count}")
        print(f"  Already labeled: {already_labeled}")
        print(f"  Errors: {errors}")
        print(f"  Results saved to: {output_csv}")
        print("-" * 60)


def main():
    """
    Main function
    """
    print()
    
    # Get input file
    input_csv = input("Enter CSV file to label (default: urls.csv): ").strip()
    if not input_csv:
        input_csv = "urls.csv"
    
    # Check if file exists
    if not os.path.exists(input_csv):
        print(f"Error: File '{input_csv}' not found!")
        return
    
    # Get API key (already loaded from .env via load_dotenv())
    api_key = os.environ.get('OPENAI_API_KEY')
    if not api_key:
        print("\n⚠ OpenAI API key not found!")
        print("\nPlease create a .env file with your API key:")
        print("  OPENAI_API_KEY=your-api-key-here")
        print("\nOr enter your API key now:")
        api_key_input = input("API Key: ").strip()
        if api_key_input:
            api_key = api_key_input
        else:
            print("\nError: No API key provided!")
            print("\nOptions:")
            print("  1. Create a .env file with: OPENAI_API_KEY=your-key")
            print("  2. Set environment variable: export OPENAI_API_KEY='your-key'")
            print("  3. Enter key when prompted")
            return
    
    print()
    
    # Run labeling
    label_urls_with_openai(input_csv, api_key=api_key)
    
    print()
    print("Done! You can now open the CSV file to see the labeled URLs.")


if __name__ == "__main__":
    main()


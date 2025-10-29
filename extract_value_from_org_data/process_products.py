#!/usr/bin/env python3
"""
Process product data from CSV and JSON files to generate structured output.

This script:
1. Loads parent SKUs from skus_parent.json
2. Reads product data from complete_accuride.csv
3. Associates child SKUs with their parent SKUs
4. Formats data as pipe-separated key-value pairs (excluding empty fields)
5. Outputs the result to final_products.json
"""

import csv
import json
from typing import Dict, List, Optional


# File paths
CSV_FILE = "complete_accuride.csv"
PARENT_FILE = "skus_parent.json"
OUTPUT_FILE = "final_products.json"

# Columns to include in the formatted description
DESCRIPTION_COLUMNS = [
    "sku",
    "name",
    "description",
    "short_description",
    "weight",
    "additional_attributes"
]


def identify_parent_skus_from_csv(csv_data: List[Dict[str, str]]) -> set:
    """
    Identify parent SKUs from CSV data.
    Parents are distinguished by having both:
    - Non-empty 'description' field
    - Non-empty 'configurable_variations' field
    """
    parent_skus = set()
    for row in csv_data:
        sku = row.get("sku", "").strip()
        description = row.get("description", "").strip()
        config_vars = row.get("configurable_variations", "").strip()
        
        # Parent products have both description and configurable_variations
        if sku and description and config_vars:
            parent_skus.add(sku)
    
    return parent_skus


def load_csv_data(filepath: str) -> List[Dict[str, str]]:
    """Load all rows from CSV file, excluding completely blank rows."""
    rows = []
    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Skip completely blank rows (family separators)
            if not any(row.values()):
                continue
            rows.append(row)
    return rows


def format_description(product_data: Dict[str, str]) -> str:
    """
    Format product data as pipe-separated key-value pairs.
    Only includes fields that are non-empty.
    
    Args:
        product_data: Dictionary containing product information
        
    Returns:
        Formatted string like "name : value ** || ** weight : value ** || ** ..."
    """
    parts = []
    for column in DESCRIPTION_COLUMNS:
        value = product_data.get(column, "").strip()
        if value:  # Only include non-empty fields
            parts.append(f"{column} : {value}")
    
    return " ** || ** ".join(parts)


def find_product_by_sku(sku: str, products: List[Dict[str, str]]) -> Optional[Dict[str, str]]:
    """Find a product in the list by its SKU."""
    for product in products:
        if product.get("sku") == sku:
            return product
    return None


def find_parent_for_child(child_sku: str, parent_skus: set, csv_data: List[Dict[str, str]]) -> Optional[str]:
    """
    Find the parent SKU for a given child SKU by checking the family attribute
    or by finding the parent that lists this child in its configurable_variations.
    
    Returns the parent SKU if found, None otherwise.
    """
    # First, check if the child has a 'family' attribute in additional_attributes
    child_row = find_product_by_sku(child_sku, csv_data)
    if child_row:
        additional_attrs = child_row.get("additional_attributes", "")
        # Look for family=XXX in additional_attributes
        import re
        family_match = re.search(r'family=([^,]+)', additional_attrs)
        if family_match:
            potential_parent = family_match.group(1).strip()
            if potential_parent in parent_skus:
                return potential_parent
    
    # Second, search through all parent products to see which one lists this child
    for parent_sku in parent_skus:
        parent_row = find_product_by_sku(parent_sku, csv_data)
        if parent_row:
            config_vars = parent_row.get("configurable_variations", "")
            # Check if this child SKU is mentioned in the parent's configurable_variations
            if f"sku={child_sku}" in config_vars:
                return parent_sku
    
    return None


def process_products(parent_skus: set, csv_data: List[Dict[str, str]]) -> Dict[str, Dict[str, str]]:
    """
    Process all products and create the output structure.
    
    Args:
        parent_skus: Set of parent SKU identifiers
        csv_data: List of all product rows from CSV
        
    Returns:
        Dictionary with child SKUs as keys and their formatted data
    """
    result = {}
    
    for product in csv_data:
        sku = product.get("sku", "").strip()
        
        # Skip empty SKUs
        if not sku:
            continue
        
        # Check if this is a parent SKU (skip it as we only process children)
        if sku in parent_skus:
            continue
        
        # Format the child's description
        child_description = format_description(product)
        
        # Try to find its parent
        parent_sku = find_parent_for_child(sku, parent_skus, csv_data)
        
        if not parent_sku:
            # This is a standalone product without a parent
            # Include it with its own data and empty parent description
            result[sku] = {
                "full_description": child_description,
                "parent_full_description": ""
            }
            continue
        
        # Find the parent product data in CSV
        parent_product = find_product_by_sku(parent_sku, csv_data)
        
        if not parent_product:
            # Parent SKU exists but not found in CSV (shouldn't happen)
            # Treat as standalone
            result[sku] = {
                "full_description": child_description,
                "parent_full_description": ""
            }
            continue
        
        # Format parent description
        parent_description = format_description(parent_product)
        
        # Add to result with both child and parent data
        result[sku] = {
            "full_description": child_description,
            "parent_full_description": parent_description
        }
    
    return result


def main():
    """Main execution function."""
    print(f"Loading CSV data from {CSV_FILE}...")
    csv_data = load_csv_data(CSV_FILE)
    print(f"  Loaded {len(csv_data)} product rows")
    
    print("\nIdentifying parent SKUs from CSV...")
    parent_skus = identify_parent_skus_from_csv(csv_data)
    print(f"  Found {len(parent_skus)} parent SKUs")
    
    print("\nProcessing child products...")
    result = process_products(parent_skus, csv_data)
    print(f"  Processed {len(result)} child products")
    
    print(f"\nWriting output to {OUTPUT_FILE}...")
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    
    print(f"\n✅ Successfully created '{OUTPUT_FILE}' with {len(result)} child products!")
    
    # Show a sample of the output
    if result:
        sample_sku = list(result.keys())[0]
        print(f"\n📋 Sample output for '{sample_sku}':")
        print(f"  full_description: {result[sample_sku]['full_description'][:100]}...")
        print(f"  parent_full_description: {result[sample_sku]['parent_full_description'][:100]}...")


if __name__ == "__main__":
    main()


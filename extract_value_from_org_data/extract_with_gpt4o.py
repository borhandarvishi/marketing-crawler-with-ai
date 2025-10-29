import json
import os
from openai import OpenAI
from tqdm import tqdm
import time
from dotenv import load_dotenv
from prompts import SYSTEM_PROMPT, create_extraction_prompt
# Initialize OpenAI client
load_dotenv()

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


PRODUCT_SCHEMA = {
    "type": "json_schema",
    "json_schema": {
        "name": "product_data",
        "strict": True,
        "schema": {
            "type": "object",
            "properties": {
                "sku": {
                    "type": ["string", "null"],
                    "description": "Unique identifier for a specific slide model variant.",
                    "examples": ["C115-20", "3832EC-18", "9301E-24"]
                },
                "parent_sku": {
                    "type": ["string", "null"],
                    "description": "The base model number representing the product family this SKU belongs to.",
                    "examples": ["115", "3832", "9301"]
                },
                "name": {
                    "type": ["string", "null"],
                    "description": "Human-readable product name describing the slide’s type and intended use.",
                    "examples": ["Light-Duty Linear Motion Slide", "Medium-Duty Full Extension Slide"]
                },
                "duty_class": {
                    "type": ["string", "null"],
                    "description": "Defines the slide’s load capacity range and performance tier: Light Duty (<140 lbs), Medium Duty (140–170 lbs), Heavy Duty (170–600 lbs), or Super Heavy Duty (>600 lbs).",
                    "enum": [
                        "Light Duty",
                        "Medium Duty",
                        "Heavy Duty",
                        "Super Heavy Duty"
                    ]
                },
                "weight": {
                    "type": ["string", "null"],
                    "description": "Approximate product weight of the slide pair, measured in pounds.",
                    "examples": ["1.05 lbs", "2.3 lbs", "5.6 lbs"]
                },
                "length": {
                    "type": ["string", "null"],
                    "description": "Total slide length when closed, including units (e.g., '20 inch', '15 mm').",
                    "examples": ["10 inch", "20 inch", "24 inch", "15 mm"]
                },
                "side_space": {
                    "type": ["string", "null"],
                    "description": "Minimum clearance required between drawer and cabinet sides for smooth operation.",
                    "enum": [
                        "Less than 0.50 inch",
                        "0.50 inch",
                        "Between 0.50 inch and 0.75 inch",
                        "0.75 inch",
                        "More than 0.75 inch"
                    ]
                },
                "mounting_type": {
                    "type": ["array", "null"],
                    "description": "Specifies how and where the slide is mounted on the drawer or cabinet. Can have multiple values.",
                    "items": {
                        "type": "string",
                        "enum": [
                            "Side Mount",
                            "Undermount",
                            "Suspended Mount",
                            "Horizontal Mount",
                            "Vertical Mount",
                            "Pocket & Bayonet",
                            "Flat Mount",
                            "Bottom-mount"
                        ]
                    }
                },
                "extension_type": {
                    "type": ["string", "null"],
                    "description": "Determines how far the drawer can extend from the cabinet when fully opened. ONLY extract if explicitly mentioned with key 'extension=' in additional_attributes or in specifications. Do NOT infer or guess this value.",
                    "enum": [
                        "3/4 Extension",
                        "Full Extension",
                        "Over-Travel"
                    ]
                },
                "load_rating": {
                    "type": ["string", "null"],
                    "description": "Maximum tested weight capacity per slide pair, including units.",
                    "examples": ["75 lbs", "132 lbs", "500 lbs"]
                },
                "movement_mechanism": {
                    "type": ["string", "null"],
                    "description": "Internal mechanism that enables slide motion and affects smoothness, noise, and durability.",
                    "examples": ["Ball Bearing", "Roller Bearing", "Friction Slide", "Linear Motion Rail"]
                },
                "feature_category": {
                    "type": ["array", "null"],
                    "description": "Primary operational features that enhance user experience and motion control. Can have multiple values.",
                    "items": {
                        "type": "string"
                    },
                    "examples": [["Easy-Close / Soft-Close", "Self-Closing"], ["Touch-Release"], ["Lock-Out"]]
                },
                "locking_mechanism": {
                    "type": ["string", "null"],
                    "description": "Specifies whether the slide includes locking positions (Lock-In, Lock-Out, Both, or None).",
                    "examples": ["Lock-In", "Lock-Out", "Both", "None"]
                },
                "special_features": {
                    "type": ["array", "null"],
                    "description": "Additional functional or environmental features that improve performance or adaptability. Can have multiple values.",
                    "items": {
                        "type": "string",
                        "enum": [
                            "Corrosion-Resistant",
                            "Detent-Out",
                            "Easy Close/Soft Close",
                            "Lock-Out",
                            "Self-Close",
                            "Touch Release",
                            "Pocket & Bayonet",
                            "Lock-In",
                            "Interlock"
                        ]
                    }
                },
                "environment_condition": {
                    "type": ["string", "null"],
                    "description": "Describes suitable environmental conditions for slide operation.",
                    "examples": ["Dry Indoor", "Humid Environment", "Outdoor", "Dusty / Industrial", "High-Temperature"]
                },
                "travel_length": {
                    "type": ["string", "null"],
                    "description": "Linear distance the drawer travels from closed to fully open position, including units.",
                    "examples": ["18 inch", "20 inch", "22 inch", "24 inch"]
                },
                "material_finish": {
                    "type": ["string", "null"],
                    "description": "Surface coating or finish applied to protect the metal and enhance appearance.",
                    "examples": ["Zinc-Plated", "Black", "Stainless Steel", "White Epoxy"]
                },
                "recommended_use": {
                    "type": ["array", "null"],
                    "description": "Suggested application types where the slide performs best. Can have multiple values.",
                    "items": {
                        "type": "string"
                    },
                    "examples": [["Kitchen Cabinets", "Office Furniture"], ["Tool Storage"], ["Vehicle Drawers", "Industrial Racks"]]
                 },
                "rohs": {
                    "type": ["integer", "null"],
                    "description": "Indicates compliance with RoHS (Restriction of Hazardous Substances) directive. Use 1 for compliant, 0 for non-compliant.",
                    "enum": [0, 1],
                    "examples": [1]
                },
                "bhma": {
                    "type": ["integer", "null"],
                    "description": "Indicates compliance with BHMA (Builders Hardware Manufacturers Association) standards. Use 1 for compliant, 0 for non-compliant.",
                    "enum": [0, 1],
                    "examples": [0]
                },
                "awi": {
                    "type": ["integer", "null"],
                    "description": "Indicates compliance with AWI (Architectural Woodwork Institute) performance standards. Use 1 for compliant, 0 for non-compliant.",
                    "enum": [0, 1],
                    "examples": [0]
                },
                "weather_resistant": {
                    "type": ["integer", "null"],
                    "description": "Specifies if the slide is resistant to weather exposure or outdoor conditions. 1 = weather-resistant, 0 = not weather-resistant.",
                    "enum": [0, 1],
                    "examples": [0]
                },
                "corrosion_resistant": {
                    "type": ["integer", "null"],
                    "description": "Specifies if the slide is resistant to corrosion. 1 = corrosion-resistant, 0 = not corrosion-resistant.",
                    "enum": [0, 1],
                    "examples": [0]
                }
            },
            "required": [
                "sku", "parent_sku", "name", "duty_class", "weight", "length",
                "side_space", "mounting_type", "extension_type", "load_rating",
                "movement_mechanism", "feature_category", "locking_mechanism",
                "special_features", "environment_condition", "travel_length",
                "material_finish", "recommended_use",
                "rohs", "bhma", "awi", "weather_resistant", "corrosion_resistant"
            ],
            "additionalProperties": False
        }
    }
}



def load_json_file(filepath):
    """Load a JSON file"""
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)

def save_json_file(filepath, data):
    """Save data to JSON file"""
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def extract_product_data(product_sku, product_info, model="gpt-4o"):
    """
    Use GPT-4o with Structured Outputs (JSON Schema) to extract data from product information.
    
    Note: The schema (PRODUCT_SCHEMA) already defines all fields, so we don't need to 
    pass DS.json separately. This simplifies the code and avoids redundancy.
    """
    # Get the full descriptions
    product_full_description = product_info.get('full_description', '')
    parent_full_description = product_info.get('parent_full_description', '')
    
    prompt = create_extraction_prompt(
        product_full_description, 
        parent_full_description
    )
    
    try:
        # Call GPT-4o with structured output using JSON Schema
        response = client.chat.completions.create(
            model=model,
            messages=[
                {
                    "role": "system",
                    "content": SYSTEM_PROMPT
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            response_format=PRODUCT_SCHEMA,
            temperature=0
        )
        
        # Parse the response
        extracted_data = json.loads(response.choices[0].message.content)
        return extracted_data
    
    except Exception as e:
        print(f"\n❌ Error processing {product_sku}: {str(e)}")
        return None

def main():
    print("🚀 Starting GPT-4o Product Data Extraction with Checkpointing")
    print("=" * 60)
    
    # Define paths
    products_path = "/Users/borhan/Desktop/PC/PROJECTS/Accuride/extract_value_from_org_data/final_products.json"
    output_path = "/Users/borhan/Desktop/PC/PROJECTS/Accuride/extract_value_from_org_data/extracted_products.json"
    
    # Load products
    print("\n📦 Loading products...")
    products = load_json_file(products_path)
    print(f"✓ Loaded {len(products)} products")
    
    # Load existing results (checkpoint resume)
    results = {}
    if os.path.exists(output_path):
        print("\n♻️  Found existing extraction file - resuming from checkpoint...")
        results = load_json_file(output_path)
        print(f"✓ Already extracted: {len(results)} products")
    else:
        print("\n🆕 No checkpoint found - starting fresh")
    
    # Calculate remaining products
    all_skus = set(products.keys())
    completed_skus = set(results.keys())
    remaining_skus = all_skus - completed_skus
    
    print(f"📊 Progress: {len(completed_skus)}/{len(products)} completed ({len(remaining_skus)} remaining)")
    
    if len(remaining_skus) == 0:
        print("\n✅ All products already extracted! Nothing to do.")
        return
    
    # Ask user if they want to process all or just a few for testing
    print("\n" + "=" * 60)
    test_mode = input("Do you want to test with just 5 products first? (y/n): ").lower().strip()
    
    if test_mode == 'y':
        products_to_process = list(remaining_skus)[:5]
        print(f"\n🧪 Testing mode: Processing 5 products")
    else:
        products_to_process = list(remaining_skus)
        print(f"\n💪 Full mode: Processing {len(remaining_skus)} remaining products")
    
    print("=" * 60)
    
    # Process products
    failed_skus = []
    checkpoint_interval = 10  # Save every 10 products
    processed_count = 0
    
    print("\n🔄 Starting extraction...\n")
    print(f"💾 Auto-saving every {checkpoint_interval} products")
    print("⚠️  Press Ctrl+C to safely interrupt (progress will be saved)\n")
    
    try:
        for sku in tqdm(products_to_process, desc="Processing products"):
            product_info = products[sku]
            extracted = extract_product_data(sku, product_info)
            
            if extracted:
                results[sku] = extracted
                processed_count += 1
                
                # Auto-save checkpoint every N products
                if processed_count % checkpoint_interval == 0:
                    save_json_file(output_path, results)
                    tqdm.write(f"💾 Checkpoint saved ({len(results)} total products)")
            else:
                failed_skus.append(sku)
            
            # Small delay to avoid rate limiting
            time.sleep(0.1)
    
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user! Saving progress...")
        save_json_file(output_path, results)
        print(f"✓ Progress saved: {len(results)} products extracted")
        print(f"💡 Run again to resume from checkpoint")
        return
    
    except Exception as e:
        print(f"\n\n❌ Error occurred: {str(e)}")
        print("💾 Saving progress before exit...")
        save_json_file(output_path, results)
        print(f"✓ Progress saved: {len(results)} products extracted")
        print(f"💡 Run again to resume from checkpoint")
        raise
    
    # Final save
    print("\n\n💾 Saving final results...")
    save_json_file(output_path, results)
    print(f"✓ Saved to: {output_path}")
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 EXTRACTION SUMMARY")
    print("=" * 60)
    print(f"✅ Successfully extracted: {len(results)} products")
    print(f"❌ Failed: {len(failed_skus)} products")
    
    if failed_skus:
        print(f"\n⚠️  Failed SKUs: {', '.join(failed_skus[:10])}")
        if len(failed_skus) > 10:
            print(f"   ... and {len(failed_skus) - 10} more")
    
    print("\n✨ Done!")

if __name__ == "__main__":
    main()


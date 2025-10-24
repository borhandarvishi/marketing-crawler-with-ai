#!/usr/bin/env python3
"""
Extract structured company data from markdown files using AI
Processes files sequentially and accumulates data to avoid duplicates
"""

import os
import json
import re
from pathlib import Path
from datetime import datetime
from openai import OpenAI
from dotenv import load_dotenv
from prompts.value_extraction_prompt import value_extraction

# Load environment variables
load_dotenv()

# Define the structured output schema
COMPANY_SCHEMA = {
    "type": "json_schema",
    "json_schema": {
        "name": "company_data",
        "strict": True,
        "schema": {
            "type": "object",
            "properties": {
                "company_name": {"type": "string"},
                "company_email": {"type": "string"},
                "company_location": {"type": "string"},
                "company_phone": {"type": "string"},
                "company_industry_type": {"type": "string"},
                "company_social_links": {
                    "type": "object",
                    "properties": {
                        "linkedin": {"type": "string"},
                        "twitter": {"type": "string"},
                        "facebook": {"type": "string"},
                        "instagram": {"type": "string"},
                        "youtube": {"type": "string"},
                        "other": {
                            "type": "array",
                            "items": {"type": "string"}
                        }
                    },
                    "required": ["linkedin", "twitter", "facebook", "instagram", "youtube", "other"],
                    "additionalProperties": False
                },
                "description": {"type": "string"},
                "company_persons": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "person_name": {"type": "string"},
                            "person_role": {"type": "string"},
                            "person_email": {"type": "string"},
                            "person_phone": {"type": "string"},
                            "person_description": {"type": "string"}
                        },
                        "required": ["person_name", "person_role", "person_email", "person_phone", "person_description"],
                        "additionalProperties": False
                    }
                }
            },
            "required": [
                "company_name",
                "company_email", 
                "company_location",
                "company_phone",
                "company_industry_type",
                "company_social_links",
                "description",
                "company_persons"
            ],
            "additionalProperties": False
        }
    }
}


def get_empty_structure():
    """Return empty company data structure"""
    return {
        "company_name": "",
        "company_email": "",
        "company_location": "",
        "company_phone": "",
        "company_industry_type": "",
        "company_social_links": {
            "linkedin": "",
            "twitter": "",
            "facebook": "",
            "instagram": "",
            "youtube": "",
            "other": []
        },
        "description": "",
        "company_persons": []
    }


def get_markdown_files(directory):
    """
    Get all markdown files in order (by numeric prefix)
    
    Args:
        directory (str): Directory containing markdown files
        
    Returns:
        list: Sorted list of markdown file paths
    """
    md_files = []
    for file in os.listdir(directory):
        if file.endswith('.md') and not file.startswith('all_'):
            md_files.append(file)
    
    # Sort by numeric prefix
    def get_number(filename):
        match = re.match(r'(\d+)_', filename)
        return int(match.group(1)) if match else 999
    
    md_files.sort(key=get_number)
    return [os.path.join(directory, f) for f in md_files]


def read_file_content(filepath):
    """Read content from a file"""
    with open(filepath, 'r', encoding='utf-8') as f:
        return f.read()


def log_openai_request(log_file, file_being_processed, prompt, response_content, tokens_used=None):
    """
    Log OpenAI request and response to a file
    
    Args:
        log_file (str): Path to log file
        file_being_processed (str): Name of file being processed
        prompt (str): The prompt sent to OpenAI
        response_content (str): The response from OpenAI
        tokens_used (dict): Token usage info
    """
    timestamp = datetime.now().isoformat()
    
    log_entry = {
        "timestamp": timestamp,
        "file_processed": file_being_processed,
        "prompt": prompt,
        "response": response_content,
        "tokens_used": tokens_used or {}
    }
    
    # Read existing logs
    if os.path.exists(log_file):
        with open(log_file, 'r', encoding='utf-8') as f:
            try:
                logs = json.load(f)
            except:
                logs = []
    else:
        logs = []
    
    # Append new log
    logs.append(log_entry)
    
    # Save logs
    with open(log_file, 'w', encoding='utf-8') as f:
        json.dump(logs, f, indent=2, ensure_ascii=False)


def save_progress(data, progress_file, file_just_processed):
    """
    Save progress after each file
    
    Args:
        data (dict): Current accumulated data
        progress_file (str): Path to progress file
        file_just_processed (str): Name of file just processed
    """
    progress_data = {
        "last_updated": datetime.now().isoformat(),
        "last_file_processed": file_just_processed,
        "data": data
    }
    
    with open(progress_file, 'w', encoding='utf-8') as f:
        json.dump(progress_data, f, indent=2, ensure_ascii=False)


def merge_company_data(current_data, new_data):
    """
    Merge new data into current data, avoiding duplicates
    
    Args:
        current_data (dict): Current accumulated data
        new_data (dict): New data from latest extraction
        
    Returns:
        dict: Merged data
    """
    # For simple string fields, update if current is empty and new has value
    string_fields = [
        'company_name', 'company_email', 'company_location', 
        'company_phone', 'company_industry_type'
    ]
    
    for field in string_fields:
        if not current_data.get(field) and new_data.get(field):
            current_data[field] = new_data[field]
    
    # Merge description (append if both exist)
    if new_data.get('description'):
        if current_data.get('description'):
            # Only append if it's different content
            if new_data['description'] not in current_data['description']:
                current_data['description'] += "\n\n" + new_data['description']
        else:
            current_data['description'] = new_data['description']
    
    # Merge social links
    for platform in ['linkedin', 'twitter', 'facebook', 'instagram', 'youtube']:
        if not current_data['company_social_links'].get(platform) and \
           new_data['company_social_links'].get(platform):
            current_data['company_social_links'][platform] = \
                new_data['company_social_links'][platform]
    
    # Merge 'other' social links (avoid duplicates)
    for link in new_data['company_social_links'].get('other', []):
        if link and link not in current_data['company_social_links']['other']:
            current_data['company_social_links']['other'].append(link)
    
    # Merge persons (avoid duplicates by name and email)
    existing_persons = set()
    for person in current_data.get('company_persons', []):
        identifier = (
            person.get('person_name', '').lower().strip(),
            person.get('person_email', '').lower().strip()
        )
        existing_persons.add(identifier)
    
    for person in new_data.get('company_persons', []):
        identifier = (
            person.get('person_name', '').lower().strip(),
            person.get('person_email', '').lower().strip()
        )
        # Only add if we have a name and it's not a duplicate
        if person.get('person_name') and identifier not in existing_persons:
            current_data['company_persons'].append(person)
            existing_persons.add(identifier)
    
    return current_data


def extract_value_from_file(client, filepath, current_data, log_file):
    """
    Extract structured data from a markdown file using GPT-4o mini
    
    Args:
        client: OpenAI client
        filepath (str): Path to markdown file
        current_data (dict): Current accumulated data
        log_file (str): Path to log file
        
    Returns:
        dict: Updated company data
    """
    filename = os.path.basename(filepath)
    print(f"\nProcessing: {filename}")
    
    # Read file content
    file_content = read_file_content(filepath)
    
    # Prepare the prompt with current data
    prompt = f"""{value_extraction}

===== CURRENT EXTRACTED DATA SO FAR =====
{json.dumps(current_data, indent=2)}

===== NEW CONTENT TO PROCESS =====
{file_content}

===== INSTRUCTIONS =====
1. Review the CURRENT EXTRACTED DATA (above)
2. Review the NEW CONTENT (above)
3. Extract any NEW information from the content that is not already in the current data
4. IMPORTANT: Return the COMPLETE updated structure including:
   - ALL existing data from "CURRENT EXTRACTED DATA"
   - PLUS any new information from "NEW CONTENT"
5. Do not duplicate information that already exists
6. For company_persons array: only add new persons not already in the list

Your response will be used as the input for the next file, so it must be complete!
"""
    
    try:
        # Call GPT-4o mini with structured output
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": "You are a precise data extraction assistant. Extract only factual information from the provided text."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            response_format=COMPANY_SCHEMA,
            temperature=0.2
        )
        
        # Parse the response
        response_content = response.choices[0].message.content
        extracted_data = json.loads(response_content)
        
        # Get token usage
        tokens_used = {
            "prompt_tokens": response.usage.prompt_tokens,
            "completion_tokens": response.usage.completion_tokens,
            "total_tokens": response.usage.total_tokens
        }
        
        # Log the request and response
        log_openai_request(log_file, filename, prompt, response_content, tokens_used)
        
        print(f"  ✓ Extracted data successfully")
        print(f"    Tokens used: {tokens_used['total_tokens']} (prompt: {tokens_used['prompt_tokens']}, completion: {tokens_used['completion_tokens']})")
        
        # Show what was found
        if extracted_data.get('company_name'):
            print(f"    Company: {extracted_data['company_name']}")
        if extracted_data.get('company_persons'):
            new_persons = len(extracted_data['company_persons']) - len(current_data.get('company_persons', []))
            if new_persons > 0:
                print(f"    Found {new_persons} new person(s)")
        
        return extracted_data
        
    except Exception as e:
        print(f"  ✗ Error: {e}")
        # Log the error
        log_openai_request(log_file, filename, prompt, f"ERROR: {str(e)}", {})
        return current_data


def save_output(data, output_file):
    """Save extracted data to file"""
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"\n✓ Saved to: {output_file}")


def main():
    """Main function"""
    print("=" * 70)
    print("AI-Powered Value Extraction from Markdown Files")
    print("=" * 70)
    
    # Check for API key
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        print("\n❌ Error: OPENAI_API_KEY not found in environment variables")
        print("Please create a .env file with your API key")
        return
    
    # Initialize OpenAI client
    client = OpenAI(api_key=api_key)
    
    # Set up paths
    content_dir = "extracted_content"
    output_file = "company_data.json"
    progress_file = "extraction_progress.json"
    log_file = "openai_requests_log.json"
    
    if not os.path.exists(content_dir):
        print(f"\n❌ Error: Directory '{content_dir}' not found!")
        return
    
    # Get all markdown files in order
    md_files = get_markdown_files(content_dir)
    
    if not md_files:
        print(f"\n❌ Error: No markdown files found in '{content_dir}'")
        return
    
    print(f"\nFound {len(md_files)} markdown files to process:")
    for idx, file in enumerate(md_files, 1):
        print(f"  {idx}. {os.path.basename(file)}")
    
    print("\n💾 Real-time saving enabled:")
    print(f"  - Progress: {progress_file}")
    print(f"  - OpenAI logs: {log_file}")
    print(f"  - Final output: {output_file}")
    
    print("\n" + "-" * 70)
    print("Starting sequential extraction...")
    print("-" * 70)
    
    # Initialize with empty structure
    accumulated_data = get_empty_structure()
    
    # Track total tokens
    total_tokens_used = 0
    
    # Process each file sequentially
    for idx, filepath in enumerate(md_files, 1):
        print(f"\n[{idx}/{len(md_files)}]", end=" ")
        
        # Extract data from current file, passing accumulated data
        # OpenAI handles merging - we use its output directly for next request
        accumulated_data = extract_value_from_file(client, filepath, accumulated_data, log_file)
        
        # Save progress after each file (real-time saving)
        save_progress(accumulated_data, progress_file, os.path.basename(filepath))
        print(f"  💾 Progress saved to {progress_file}")
    
    # Save final output
    print("\n" + "-" * 70)
    print("Extraction complete!")
    print("-" * 70)
    
    # Calculate total tokens from log
    if os.path.exists(log_file):
        with open(log_file, 'r', encoding='utf-8') as f:
            logs = json.load(f)
            total_tokens_used = sum(log.get('tokens_used', {}).get('total_tokens', 0) for log in logs)
    
    # Print summary
    print("\n📊 Summary:")
    if accumulated_data.get('company_name'):
        print(f"  Company Name: {accumulated_data['company_name']}")
    if accumulated_data.get('company_email'):
        print(f"  Email: {accumulated_data['company_email']}")
    if accumulated_data.get('company_phone'):
        print(f"  Phone: {accumulated_data['company_phone']}")
    if accumulated_data.get('company_location'):
        print(f"  Location: {accumulated_data['company_location']}")
    if accumulated_data.get('company_industry_type'):
        print(f"  Industry: {accumulated_data['company_industry_type']}")
    
    persons_count = len(accumulated_data.get('company_persons', []))
    if persons_count > 0:
        print(f"  Persons Found: {persons_count}")
        for person in accumulated_data['company_persons']:
            if person.get('person_name'):
                role = person.get('person_role', 'N/A')
                description = person.get('person_description', 'N/A')
                print(f"    - {person['person_name']} ({role}) {description}")
    
    social_count = sum(1 for v in accumulated_data['company_social_links'].values() 
                      if v and v != [])
    if social_count > 0:
        print(f"  Social Links: {social_count} platform(s)")
    
    print(f"\n💰 Total tokens used: {total_tokens_used:,}")
    estimated_cost = (total_tokens_used / 1_000_000) * 0.15  # $0.15 per 1M tokens for gpt-4o-mini
    print(f"   Estimated cost: ${estimated_cost:.4f}")
    
    # Save the final result
    save_output(accumulated_data, output_file)
    
    print("\n📋 Files created:")
    print(f"  ✓ {output_file} - Final company data")
    print(f"  ✓ {progress_file} - Latest progress snapshot")
    print(f"  ✓ {log_file} - Complete OpenAI request/response log")
    
    print("\n" + "=" * 70)
    print("✨ Done! Company data has been extracted and saved.")
    print("=" * 70)


if __name__ == "__main__":
    main()


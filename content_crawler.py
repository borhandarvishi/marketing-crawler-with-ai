#!/usr/bin/env python3
"""
Extract clean content from URLs marked as useful
Reads CSV file and extracts readable text content from pages
"""

import csv
import json
import os
import time
import re
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse


class ContentExtractor:
    def __init__(self, output_dir="extracted_content", max_retries=5, timeout=30):
        """
        Initialize the content extractor
        
        Args:
            output_dir (str): Directory to save extracted content
            max_retries (int): Maximum number of retry attempts
            timeout (int): Request timeout in seconds
        """
        self.output_dir = output_dir
        self.max_retries = max_retries
        self.timeout = timeout
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
    
    def clean_text(self, text):
        """
        Clean extracted text
        
        Args:
            text (str): Raw text
            
        Returns:
            str: Cleaned text
        """
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text)
        # Remove leading/trailing whitespace
        text = text.strip()
        return text
    
    def extract_metadata(self, soup, url):
        """
        Extract metadata from the page
        
        Args:
            soup: BeautifulSoup object
            url: Page URL
            
        Returns:
            dict: Metadata dictionary
        """
        metadata = {
            'url': url,
            'title': '',
            'description': '',
            'keywords': '',
            'extracted_at': datetime.now().isoformat()
        }
        
        # Extract title
        title_tag = soup.find('title')
        if title_tag:
            metadata['title'] = self.clean_text(title_tag.get_text())
        
        # Extract meta description
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        if meta_desc and meta_desc.get('content'):
            metadata['description'] = self.clean_text(meta_desc.get('content'))
        
        # Extract meta keywords
        meta_keywords = soup.find('meta', attrs={'name': 'keywords'})
        if meta_keywords and meta_keywords.get('content'):
            metadata['keywords'] = self.clean_text(meta_keywords.get('content'))
        
        return metadata
    
    def extract_contact_info(self, soup, text):
        """
        Extract contact information from page
        
        Args:
            soup: BeautifulSoup object
            text: Page text content
            
        Returns:
            dict: Contact information
        """
        contact_info = {
            'emails': [],
            'phones': [],
            'social_links': []
        }
        
        # Extract emails
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        emails = re.findall(email_pattern, text)
        contact_info['emails'] = list(set(emails))
        
        # Extract phone numbers (various formats)
        phone_pattern = r'(\+?\d{1,3}[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}'
        phones = re.findall(phone_pattern, text)
        contact_info['phones'] = [self.clean_text(''.join(p) if isinstance(p, tuple) else p) for p in phones]
        contact_info['phones'] = list(set(contact_info['phones']))[:5]  # Limit to 5
        
        # Extract social media links
        social_domains = ['linkedin.com', 'twitter.com', 'facebook.com', 'instagram.com', 
                         'youtube.com', 'github.com']
        for link in soup.find_all('a', href=True):
            href = link.get('href')
            if any(domain in href for domain in social_domains):
                contact_info['social_links'].append(href)
        
        contact_info['social_links'] = list(set(contact_info['social_links']))
        
        return contact_info
    
    def extract_clean_content(self, url):
        """
        Extract clean, readable content from a URL with retry logic
        
        Args:
            url (str): URL to extract content from
            
        Returns:
            dict: Extracted content with metadata
        """
        # Try multiple times with exponential backoff
        for attempt in range(self.max_retries):
            try:
                if attempt == 0:
                    print(f"Fetching: {url}")
                else:
                    print(f"  🔄 Retry attempt {attempt + 1}/{self.max_retries}")
                
                # Make request
                response = requests.get(url, headers=self.headers, timeout=self.timeout)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Remove unwanted elements
                for element in soup(['script', 'style', 'nav', 'footer', 'header', 
                                    'aside', 'iframe', 'noscript', 'svg']):
                    element.decompose()
                
                # Remove comments
                for comment in soup.find_all(string=lambda text: isinstance(text, str) and text.strip().startswith('<!--')):
                    comment.extract()
                
                # Extract metadata
                metadata = self.extract_metadata(soup, url)
                
                # Extract main content
                # Try to find main content area
                main_content = (
                    soup.find('main') or 
                    soup.find('article') or 
                    soup.find('div', class_=re.compile(r'content|main', re.I)) or
                    soup.find('body')
                )
                
                if not main_content:
                    main_content = soup
                
                # Extract all text
                text_content = main_content.get_text(separator='\n', strip=True)
                
                # Clean the text
                lines = [line.strip() for line in text_content.split('\n') if line.strip()]
                # Remove duplicate consecutive lines
                cleaned_lines = []
                prev_line = None
                for line in lines:
                    if line != prev_line:
                        cleaned_lines.append(line)
                    prev_line = line
                
                text_content = '\n'.join(cleaned_lines)
                
                # Extract headings structure
                headings = []
                for heading in main_content.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6']):
                    headings.append({
                        'level': heading.name,
                        'text': self.clean_text(heading.get_text())
                    })
                
                # Extract links
                links = []
                for link in main_content.find_all('a', href=True):
                    link_text = self.clean_text(link.get_text())
                    if link_text:  # Only add links with text
                        links.append({
                            'text': link_text,
                            'href': link.get('href')
                        })
                
                # Extract contact information
                contact_info = self.extract_contact_info(soup, text_content)
                
                # Compile result
                result = {
                    'metadata': metadata,
                    'content': text_content,
                    'headings': headings,
                    'links': links[:20],  # Limit to first 20 links
                    'contact_info': contact_info,
                    'word_count': len(text_content.split()),
                    'extraction_successful': True,
                    'attempts': attempt + 1
                }
                
                print(f"  ✓ Extracted {result['word_count']} words")
                return result
                
            except requests.exceptions.Timeout as e:
                print(f"  ⚠️  Timeout error (attempt {attempt + 1}/{self.max_retries}): {e}")
                if attempt < self.max_retries - 1:
                    # Exponential backoff: 2, 4, 8, 16 seconds
                    wait_time = 2 ** (attempt + 1)
                    print(f"  ⏳ Waiting {wait_time} seconds before retry...")
                    time.sleep(wait_time)
                else:
                    # Final attempt failed
                    print(f"  ❌ Failed after {self.max_retries} attempts")
                    return {
                        'metadata': {'url': url, 'extracted_at': datetime.now().isoformat()},
                        'content': '',
                        'error': f'Timeout after {self.max_retries} attempts: {str(e)}',
                        'extraction_successful': False,
                        'attempts': self.max_retries
                    }
                    
            except requests.exceptions.RequestException as e:
                print(f"  ⚠️  Request error (attempt {attempt + 1}/{self.max_retries}): {e}")
                if attempt < self.max_retries - 1:
                    # Exponential backoff
                    wait_time = 2 ** (attempt + 1)
                    print(f"  ⏳ Waiting {wait_time} seconds before retry...")
                    time.sleep(wait_time)
                else:
                    # Final attempt failed
                    print(f"  ❌ Failed after {self.max_retries} attempts")
                    return {
                        'metadata': {'url': url, 'extracted_at': datetime.now().isoformat()},
                        'content': '',
                        'error': f'Request failed after {self.max_retries} attempts: {str(e)}',
                        'extraction_successful': False,
                        'attempts': self.max_retries
                    }
                    
            except Exception as e:
                print(f"  ✗ Extraction error: {e}")
                return {
                    'metadata': {'url': url, 'extracted_at': datetime.now().isoformat()},
                    'content': '',
                    'error': str(e),
                    'extraction_successful': False,
                    'attempts': attempt + 1
                }
    
    def save_as_json(self, data, filename):
        """Save extracted data as JSON"""
        filepath = os.path.join(self.output_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        return filepath
    
    def save_as_markdown(self, data, filename):
        """Save extracted data as Markdown"""
        filepath = os.path.join(self.output_dir, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            # Write header
            f.write(f"# {data['metadata'].get('title', 'Untitled')}\n\n")
            f.write(f"**URL:** {data['metadata']['url']}\n\n")
            f.write(f"**Extracted:** {data['metadata']['extracted_at']}\n\n")
            
            if data['metadata'].get('description'):
                f.write(f"**Description:** {data['metadata']['description']}\n\n")
            
            # Write contact info
            if data.get('contact_info'):
                contact = data['contact_info']
                if contact.get('emails') or contact.get('phones'):
                    f.write("## Contact Information\n\n")
                    if contact.get('emails'):
                        f.write(f"**Emails:** {', '.join(contact['emails'])}\n\n")
                    if contact.get('phones'):
                        f.write(f"**Phones:** {', '.join(contact['phones'])}\n\n")
                    if contact.get('social_links'):
                        f.write("**Social Links:**\n")
                        for link in contact['social_links']:
                            f.write(f"- {link}\n")
                        f.write("\n")
            
            # Write main content
            f.write("## Content\n\n")
            f.write(data['content'])
            f.write("\n")
        
        return filepath
    
    def save_as_text(self, data, filename):
        """Save extracted data as plain text"""
        filepath = os.path.join(self.output_dir, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            # Write header
            f.write(f"Title: {data['metadata'].get('title', 'Untitled')}\n")
            f.write(f"URL: {data['metadata']['url']}\n")
            f.write(f"Extracted: {data['metadata']['extracted_at']}\n")
            f.write("=" * 80 + "\n\n")
            
            # Write contact info
            if data.get('contact_info'):
                contact = data['contact_info']
                if contact.get('emails') or contact.get('phones'):
                    f.write("CONTACT INFORMATION\n")
                    f.write("-" * 80 + "\n")
                    if contact.get('emails'):
                        f.write(f"Emails: {', '.join(contact['emails'])}\n")
                    if contact.get('phones'):
                        f.write(f"Phones: {', '.join(contact['phones'])}\n")
                    if contact.get('social_links'):
                        f.write(f"Social: {', '.join(contact['social_links'])}\n")
                    f.write("\n")
            
            # Write content
            f.write("CONTENT\n")
            f.write("-" * 80 + "\n")
            f.write(data['content'])
            f.write("\n")
        
        return filepath


def extract_from_csv(csv_file, output_format='all'):
    """
    Extract content from URLs marked as useful in CSV
    
    Args:
        csv_file (str): Path to CSV file
        output_format (str): 'json', 'markdown', 'text', or 'all'
    """
    print("=" * 60)
    print("Content Extraction from Useful URLs")
    print("=" * 60)
    print(f"Reading from: {csv_file}")
    print(f"Output format: {output_format}")
    print("-" * 60)
    
    # Read CSV and filter for useful URLs
    useful_urls = []
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get('isUseful', '').strip().lower() == 'true':
                useful_urls.append(row['url'])
    
    print(f"Found {len(useful_urls)} useful URLs to extract")
    print(f"Retry strategy: Up to 5 attempts with exponential backoff")
    print(f"Timeout: 30 seconds per request")
    print("-" * 60)
    
    if not useful_urls:
        print("No URLs marked as useful. Exiting.")
        return
    
    # Initialize extractor with retry settings
    extractor = ContentExtractor(max_retries=5, timeout=30)
    
    # Extract content from each URL
    all_data = []
    successful = 0
    failed = 0
    failed_urls = []
    
    for idx, url in enumerate(useful_urls, 1):
        print(f"\n[{idx}/{len(useful_urls)}] Processing: {url}")
        
        # Extract content
        data = extractor.extract_clean_content(url)
        all_data.append(data)
        
        if data['extraction_successful']:
            successful += 1
            
            # Generate filename from URL
            parsed = urlparse(url)
            filename_base = parsed.path.strip('/').replace('/', '_') or 'homepage'
            filename_base = re.sub(r'[^\w\-_]', '_', filename_base)
            filename_base = f"{idx}_{filename_base}"
            
            # Save in requested format(s)
            if output_format in ['json', 'all']:
                json_path = extractor.save_as_json(data, f"{filename_base}.json")
                print(f"  📄 Saved JSON: {json_path}")
            
            if output_format in ['markdown', 'all']:
                md_path = extractor.save_as_markdown(data, f"{filename_base}.md")
                print(f"  📝 Saved Markdown: {md_path}")
            
            if output_format in ['text', 'all']:
                txt_path = extractor.save_as_text(data, f"{filename_base}.txt")
                print(f"  📃 Saved Text: {txt_path}")
        else:
            failed += 1
            failed_urls.append({
                'url': url,
                'error': data.get('error', 'Unknown error'),
                'attempts': data.get('attempts', 0)
            })
        
        # Be polite - small delay between requests
        time.sleep(1)
    
    # Save combined data
    print("\n" + "-" * 60)
    print("Saving combined data...")
    
    combined_path = extractor.save_as_json(all_data, "all_extracted_content.json")
    print(f"📦 Combined JSON: {combined_path}")
    
    # Save failed URLs to a separate file for retry
    if failed_urls:
        failed_path = os.path.join(extractor.output_dir, "failed_urls.json")
        with open(failed_path, 'w', encoding='utf-8') as f:
            json.dump(failed_urls, f, indent=2, ensure_ascii=False)
        print(f"⚠️  Failed URLs list: {failed_path}")
        
        # Also save as CSV for easy retry
        failed_csv_path = os.path.join(extractor.output_dir, "failed_urls.csv")
        with open(failed_csv_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['url', 'error', 'attempts'])
            for item in failed_urls:
                writer.writerow([item['url'], item['error'], item['attempts']])
        print(f"⚠️  Failed URLs CSV: {failed_csv_path}")
    
    # Print summary
    print("-" * 60)
    print("SUMMARY:")
    print(f"  Total URLs: {len(useful_urls)}")
    print(f"  ✅ Successful: {successful}")
    print(f"  ❌ Failed: {failed}")
    if failed > 0:
        print(f"\n  Failed URLs have been saved to:")
        print(f"    - {extractor.output_dir}/failed_urls.json")
        print(f"    - {extractor.output_dir}/failed_urls.csv")
        print(f"  You can review and retry these URLs later.")
    print(f"\n  Output directory: {extractor.output_dir}")
    print("-" * 60)


def main():
    """Main function"""
    print()
    
    # Get input file
    csv_file = input("Enter CSV file (default: urls.csv): ").strip() or "urls.csv"
    
    if not os.path.exists(csv_file):
        print(f"Error: File '{csv_file}' not found!")
        return
    
    # Get output format
    print("\nOutput format options:")
    print("  1. JSON (structured data)")
    print("  2. Markdown (readable format)")
    print("  3. Text (plain text)")
    print("  4. All formats")
    
    format_choice = input("\nChoose format (1-4, default: 4): ").strip() or "4"
    
    format_map = {
        '1': 'json',
        '2': 'markdown',
        '3': 'text',
        '4': 'all'
    }
    
    output_format = format_map.get(format_choice, 'all')
    
    print()
    
    # Extract content
    extract_from_csv(csv_file, output_format)
    
    print()
    print("Done! Check the 'extracted_content' directory for results.")


if __name__ == "__main__":
    main()


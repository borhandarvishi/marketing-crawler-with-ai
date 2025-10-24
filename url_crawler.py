#!/usr/bin/env python3
"""
Web Crawler - Extract all unique URLs from a website
Supports single URL mode or batch mode from CSV file
"""

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import time
import csv
import os
import re
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed


class WebCrawler:
    def __init__(self, base_url, output_file="urls.csv", max_depth=3, max_urls=500):
        """
        Initialize the web crawler
        
        Args:
            base_url (str): The starting URL to crawl
            output_file (str): The output CSV file to save URLs
            max_depth (int): Maximum depth to crawl (default: 3)
            max_urls (int): Maximum number of URLs to collect (default: 500)
        """
        self.base_url = base_url
        self.domain = urlparse(base_url).netloc
        self.visited_urls = set()
        self.urls_to_visit = deque([(base_url, 0)])  # (url, depth)
        self.output_file = output_file
        self.file_handle = None
        self.csv_writer = None
        self.max_depth = max_depth
        self.max_urls = max_urls
        
        # Common tracking parameters to remove
        self.tracking_params = {
            'utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content',
            'fbclid', 'gclid', 'msclkid', 'mc_cid', 'mc_eid',
            'ref', 'source', 'promo', '_ga', '_gl', 'referrer'
        }
        
        # URL patterns to skip (common noise)
        self.skip_patterns = [
            '/tag/', '/tags/', '/category/', '/categories/',
            '/author/', '/user/', '/search/', '/page/',
            '/filter/', '/sort/', '/archive/', '/date/',
            '?page=', '&page=', '/feed/', '/rss/',
            '/print/', '/share/', '/embed/', '/amp/'
        ]
        
    def clean_url(self, url):
        """
        Clean URL by removing tracking parameters and normalizing
        
        Args:
            url (str): The URL to clean
            
        Returns:
            str: Cleaned URL
        """
        parsed = urlparse(url)
        
        # Parse query parameters
        if parsed.query:
            from urllib.parse import parse_qs, urlencode
            params = parse_qs(parsed.query, keep_blank_values=True)
            
            # Remove tracking parameters
            cleaned_params = {
                k: v for k, v in params.items() 
                if k not in self.tracking_params
            }
            
            # Rebuild query string
            if cleaned_params:
                query = urlencode(cleaned_params, doseq=True)
                clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}?{query}"
            else:
                clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        else:
            clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        
        # Remove trailing slash for consistency (except for root)
        if clean_url.endswith('/') and len(parsed.path) > 1:
            clean_url = clean_url.rstrip('/')
        
        return clean_url
    
    def should_skip_url(self, url):
        """
        Check if URL matches skip patterns
        
        Args:
            url (str): The URL to check
            
        Returns:
            bool: True if URL should be skipped
        """
        url_lower = url.lower()
        for pattern in self.skip_patterns:
            if pattern in url_lower:
                return True
        return False
    
    def is_valid_url(self, url):
        """
        Check if a URL is valid and accessible
        
        Args:
            url (str): The URL to validate
            
        Returns:
            bool: True if URL is valid and returns 200, False otherwise
        """
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            response = requests.head(url, headers=headers, timeout=10, allow_redirects=True)
            # Accept 200 (OK) and 405 (Method Not Allowed - some servers block HEAD)
            if response.status_code == 405:
                # Try GET request if HEAD is not allowed
                response = requests.get(url, headers=headers, timeout=10, allow_redirects=True)
            return response.status_code == 200
        except:
            return False
    
    def get_links(self, url):
        """
        Extract all links from a given URL
        
        Args:
            url (str): The URL to extract links from
            
        Returns:
            set: A set of absolute URLs found on the page
        """
        links = set()
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            response = requests.get(url, headers=headers, timeout=10, allow_redirects=True)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find all anchor tags
            for link in soup.find_all('a', href=True):
                href = link.get('href')
                
                # Skip javascript, mailto, tel, and anchor-only links
                if href.startswith(('#', 'javascript:', 'mailto:', 'tel:')):
                    continue
                
                # Convert relative URLs to absolute URLs
                absolute_url = urljoin(url, href)
                
                # Parse the URL
                parsed_url = urlparse(absolute_url)
                
                # Only include URLs from the same domain
                if parsed_url.netloc == self.domain:
                    # Clean the URL (remove tracking params)
                    clean_url = self.clean_url(absolute_url)
                    
                    # Skip if matches skip patterns
                    if self.should_skip_url(clean_url):
                        continue
                    
                    links.add(clean_url)
                    
        except requests.exceptions.RequestException as e:
            print(f"Error fetching {url}: {e}")
        except Exception as e:
            print(f"Error parsing {url}: {e}")
            
        return links
    
    def crawl(self, validate_urls=True):
        """
        Crawl the website starting from the base URL
        
        Args:
            validate_urls (bool): If True, validate URLs before saving (slower but more accurate)
        """
        print(f"Starting crawl from: {self.base_url}")
        print(f"Domain: {self.domain}")
        print(f"Max depth: {self.max_depth}")
        print(f"Max URLs: {self.max_urls}")
        print(f"Saving URLs in real-time to: {self.output_file}")
        print(f"URL Validation: {'Enabled' if validate_urls else 'Disabled'}")
        print("-" * 60)
        
        skipped_count = 0
        skipped_depth = 0
        skipped_patterns = 0
        
        # Open file for writing in real-time
        try:
            self.file_handle = open(self.output_file, 'w', encoding='utf-8', newline='')
            self.csv_writer = csv.writer(self.file_handle)
            
            # Write header row
            self.csv_writer.writerow(['url', 'isUseful'])
            self.file_handle.flush()
            
            while self.urls_to_visit:
                # Check if we've hit the max URLs limit
                valid_urls_count = len(self.visited_urls) - skipped_count
                if valid_urls_count >= self.max_urls:
                    print(f"\n⚠️  Reached maximum URL limit ({self.max_urls})")
                    break
                
                current_url, current_depth = self.urls_to_visit.popleft()
                
                if current_url in self.visited_urls:
                    continue
                
                # Check depth limit
                if current_depth > self.max_depth:
                    skipped_depth += 1
                    continue
                
                # Validate URL if enabled
                if validate_urls:
                    print(f"Validating [depth {current_depth}]: {current_url}")
                    if not self.is_valid_url(current_url):
                        print(f"  ✗ Skipped (invalid or inaccessible)")
                        skipped_count += 1
                        self.visited_urls.add(current_url)  # Mark as visited to avoid re-checking
                        continue
                    print(f"  ✓ Valid")
                else:
                    print(f"Crawling [{valid_urls_count + 1}] [depth {current_depth}]: {current_url}")
                    
                self.visited_urls.add(current_url)
                
                # Save URL to CSV file immediately (only valid URLs)
                self.csv_writer.writerow([current_url, ''])  # url, empty isUseful
                self.file_handle.flush()  # Force write to disk
                
                # Get all links from the current page (only if not at max depth)
                if current_depth < self.max_depth:
                    links = self.get_links(current_url)
                    
                    # Add new links to the queue with incremented depth
                    for link in links:
                        if link not in self.visited_urls:
                            # Check if link is already in queue
                            already_queued = any(url == link for url, _ in self.urls_to_visit)
                            if not already_queued:
                                self.urls_to_visit.append((link, current_depth + 1))
                
                # Be polite - add a small delay between requests
                time.sleep(0.5)
            
            print("-" * 60)
            print(f"Crawling complete!")
            print(f"Valid URLs found: {len(self.visited_urls) - skipped_count}")
            if validate_urls:
                print(f"Invalid URLs skipped: {skipped_count}")
            if skipped_depth > 0:
                print(f"URLs skipped (depth limit): {skipped_depth}")
            
        except KeyboardInterrupt:
            print("\n" + "-" * 60)
            print(f"Crawling interrupted by user!")
            print(f"Valid URLs found: {len(self.visited_urls) - skipped_count}")
            if validate_urls:
                print(f"Invalid URLs skipped: {skipped_count}")
            
        finally:
            # Always close the file handle
            if self.file_handle:
                self.file_handle.close()
                print(f"URLs saved to: {self.output_file}")
        


def generate_filename(url):
    """
    Generate a meaningful filename from a URL
    
    Args:
        url (str): The URL to generate filename from
        
    Returns:
        str: A clean filename based on the domain
    """
    parsed = urlparse(url)
    domain = parsed.netloc
    
    # Remove www. prefix if present
    domain = domain.replace('www.', '')
    
    # Replace dots with underscores
    domain = domain.replace('.', '_')
    
    # Remove any other special characters
    domain = re.sub(r'[^\w\-]', '_', domain)
    
    return f"{domain}_urls.csv"


def crawl_single_site(url, validate_urls=True, max_depth=3, max_urls=500):
    """
    Crawl a single site and save to its own file
    
    Args:
        url (str): The base URL to crawl
        validate_urls (bool): Whether to validate URLs before saving
        max_depth (int): Maximum crawl depth
        max_urls (int): Maximum number of URLs to collect
        
    Returns:
        dict: Results with url, output_file, and count
    """
    output_file = generate_filename(url)
    
    print(f"\n{'=' * 70}")
    print(f"Starting crawl for: {url}")
    print(f"Output file: {output_file}")
    print(f"{'=' * 70}")
    
    try:
        crawler = WebCrawler(url, output_file, max_depth=max_depth, max_urls=max_urls)
        crawler.crawl(validate_urls=validate_urls)
        
        url_count = len(crawler.visited_urls)
        
        return {
            'url': url,
            'output_file': output_file,
            'count': url_count,
            'success': True,
            'error': None
        }
    except Exception as e:
        return {
            'url': url,
            'output_file': output_file,
            'count': 0,
            'success': False,
            'error': str(e)
        }


def crawl_from_csv(csv_file, validate_urls=True, max_workers=3, max_depth=3, max_urls=500):
    """
    Crawl multiple sites from a CSV file in parallel
    
    Args:
        csv_file (str): Path to CSV file with base URLs
        validate_urls (bool): Whether to validate URLs before saving
        max_workers (int): Maximum number of parallel crawlers
        max_depth (int): Maximum crawl depth
        max_urls (int): Maximum number of URLs to collect per site
    """
    print("=" * 70)
    print("Batch Web Crawler - Processing multiple sites in parallel")
    print("=" * 70)
    
    # Read base URLs from CSV
    base_urls = []
    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                url = row.get('url', '').strip()
                if url and url.startswith(('http://', 'https://')):
                    base_urls.append(url)
    except FileNotFoundError:
        print(f"\n❌ Error: File '{csv_file}' not found!")
        return
    except Exception as e:
        print(f"\n❌ Error reading CSV: {e}")
        return
    
    if not base_urls:
        print(f"\n❌ No valid URLs found in '{csv_file}'")
        print("Make sure the CSV has a 'url' column with valid HTTP/HTTPS URLs")
        return
    
    print(f"\nFound {len(base_urls)} URL(s) to crawl:")
    for idx, url in enumerate(base_urls, 1):
        print(f"  {idx}. {url} → {generate_filename(url)}")
    
    print(f"\nCrawling with {max_workers} parallel workers...")
    print(f"Max depth: {max_depth} | Max URLs per site: {max_urls}")
    print(f"URL Validation: {'Enabled' if validate_urls else 'Disabled'}")
    print("-" * 70)
    
    # Crawl sites in parallel
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all crawl jobs
        future_to_url = {
            executor.submit(crawl_single_site, url, validate_urls, max_depth, max_urls): url 
            for url in base_urls
        }
        
        # Process completed jobs
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                results.append({
                    'url': url,
                    'output_file': generate_filename(url),
                    'count': 0,
                    'success': False,
                    'error': str(e)
                })
    
    # Print summary
    print("\n" + "=" * 70)
    print("CRAWLING SUMMARY")
    print("=" * 70)
    
    successful = [r for r in results if r['success']]
    failed = [r for r in results if not r['success']]
    
    if successful:
        print("\n✅ Successfully crawled:")
        for result in successful:
            print(f"  • {result['url']}")
            print(f"    → {result['output_file']} ({result['count']} URLs)")
    
    if failed:
        print("\n❌ Failed:")
        for result in failed:
            print(f"  • {result['url']}")
            print(f"    Error: {result['error']}")
    
    print("\n" + "-" * 70)
    print(f"Total: {len(successful)}/{len(results)} sites crawled successfully")
    print("=" * 70)


def main():
    """
    Main function to run the crawler
    Supports both single URL mode and batch mode from CSV
    """
    print("=" * 70)
    print("Web Crawler - Extract all unique URLs from websites")
    print("=" * 70)
    print()
    print("Choose mode:")
    print("  1. Single URL (interactive)")
    print("  2. Batch mode (from base_urls.csv)")
    print()
    
    mode = input("Enter mode (1 or 2, default: 1): ").strip() or "1"
    
    # Ask about validation
    validate = input("\nValidate URLs before saving? (slower but accurate) [Y/n]: ").strip().lower()
    validate_urls = validate != 'n'
    
    print()
    
    # Get crawl limits
    print("Crawl limits (press Enter for defaults):")
    max_depth_input = input("  Max depth (default: 3): ").strip()
    max_depth = int(max_depth_input) if max_depth_input.isdigit() else 3
    
    max_urls_input = input("  Max URLs per site (default: 500): ").strip()
    max_urls = int(max_urls_input) if max_urls_input.isdigit() else 500
    
    if mode == "2":
        # Batch mode from CSV
        csv_file = input("\nEnter CSV file (default: base_urls.csv): ").strip() or "base_urls.csv"
        max_workers = input("Max parallel crawlers (default: 3): ").strip()
        max_workers = int(max_workers) if max_workers.isdigit() else 3
        
        crawl_from_csv(csv_file, validate_urls=validate_urls, max_workers=max_workers, 
                      max_depth=max_depth, max_urls=max_urls)
    else:
        # Single URL mode
        base_url = input("\nEnter the base URL (e.g., https://techstrata.com): ").strip()
        
        # Validate URL
        if not base_url.startswith(('http://', 'https://')):
            print("Error: URL must start with http:// or https://")
            return
        
        # Get output filename
        output_file = input("Enter output filename (default: urls.csv): ").strip()
        if not output_file:
            output_file = "urls.csv"
        
        print()
        
        # Create and run crawler
        crawler = WebCrawler(base_url, output_file, max_depth=max_depth, max_urls=max_urls)
        crawler.crawl(validate_urls=validate_urls)
    
    print()
    print("Done!")


if __name__ == "__main__":
    main()


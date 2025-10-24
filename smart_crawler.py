#!/usr/bin/env python3
"""
Smart Web Crawler - Focuses on main/important pages only
Uses sitemap + intelligent link detection
"""

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, parse_qs, urlencode
import time
import csv
import os
import re
import xml.etree.ElementTree as ET
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed


class SmartCrawler:
    def __init__(self, base_url, output_file="urls.csv", max_urls=100):
        """
        Initialize the smart crawler
        
        Args:
            base_url (str): The starting URL to crawl
            output_file (str): The output CSV file to save URLs
            max_urls (int): Maximum number of URLs to collect (default: 100)
        """
        self.base_url = base_url
        self.domain = urlparse(base_url).netloc
        self.discovered_urls = {}  # url: priority_score
        self.output_file = output_file
        self.max_urls = max_urls
        
        # Tracking parameters to remove
        self.tracking_params = {
            'utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content',
            'fbclid', 'gclid', 'msclkid', 'mc_cid', 'mc_eid',
            'ref', 'source', 'promo', '_ga', '_gl', 'referrer'
        }
        
        # URL patterns to skip (only files and technical endpoints)
        self.skip_patterns = [
            # File types
            '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx',
            '.zip', '.rar', '.tar', '.gz',
            '.jpg', '.jpeg', '.png', '.gif', '.svg', '.ico', '.webp',
            '.mp3', '.mp4', '.avi', '.mov', '.wmv',
            '.css', '.js', '.json', '.xml',
            # Technical endpoints
            '/wp-json/', '/wp-content/uploads/', '/wp-admin/',
            '/api/', '/feed/', '/rss/',
            # Tracking and actions
            '?replytocom=', '?attachment_id=', '/trackback/'
        ]
        
        # Crawl depth tracker
        self.url_depths = {}  # url: depth
        self.max_depth = 3  # How deep to crawl
        
    def clean_url(self, url):
        """Clean URL by removing tracking parameters"""
        parsed = urlparse(url)
        
        if parsed.query:
            params = parse_qs(parsed.query, keep_blank_values=True)
            cleaned_params = {
                k: v for k, v in params.items() 
                if k not in self.tracking_params
            }
            
            if cleaned_params:
                query = urlencode(cleaned_params, doseq=True)
                clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}?{query}"
            else:
                clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        else:
            clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        
        # Remove trailing slash (except root)
        if clean_url.endswith('/') and len(parsed.path) > 1:
            clean_url = clean_url.rstrip('/')
        
        return clean_url
    
    def should_skip_url(self, url):
        """Check if URL matches skip patterns"""
        url_lower = url.lower()
        for pattern in self.skip_patterns:
            if pattern in url_lower:
                return True
        return False
    
    def calculate_priority(self, url, context='', depth=0):
        """
        Calculate priority score for a URL (language-agnostic)
        Higher score = more important page
        
        Args:
            url (str): The URL to score
            context (str): Context where link was found (nav, footer, body)
            depth (int): Crawl depth level
            
        Returns:
            int: Priority score (0-100)
        """
        path = urlparse(url).path
        path_parts = [p for p in path.strip('/').split('/') if p]
        
        # Homepage gets highest priority
        if path in ['/', '']:
            return 100
        
        # Base score starts at 50
        score = 50
        
        # Context-based scoring (where the link was found)
        if context == 'nav':
            score += 30  # Navigation links are very important
        elif context == 'footer':
            score += 20  # Footer links are important
        elif context == 'homepage':
            score += 25  # Links from homepage are important
        else:
            score += 0  # Body links get base score
        
        # Depth-based scoring (shallower is better, universal for all sites)
        url_depth = len(path_parts)
        if url_depth == 1:
            score += 20
        elif url_depth == 2:
            score += 10
        elif url_depth == 3:
            score += 0
        elif url_depth == 4:
            score -= 10
        else:
            score -= 20
        
        # Crawl depth penalty (how many hops from homepage)
        score -= (depth * 5)
        
        # Slight penalty for query parameters
        if '?' in url:
            score -= 5
        
        return max(0, min(100, score))  # Keep between 0-100
    
    def parse_sitemap(self):
        """
        Try to parse sitemap.xml to get URLs
        
        Returns:
            list: List of URLs from sitemap
        """
        print("🗺️  Checking for sitemap...")
        
        sitemap_urls = [
            f"{self.base_url}/sitemap.xml",
            f"{self.base_url}/sitemap_index.xml",
            f"https://{self.domain}/sitemap.xml",
            f"https://{self.domain}/sitemap_index.xml",
        ]
        
        found_urls = []
        
        for sitemap_url in sitemap_urls:
            try:
                response = requests.get(sitemap_url, timeout=10)
                if response.status_code == 200:
                    print(f"  ✓ Found sitemap: {sitemap_url}")
                    
                    # Parse XML
                    root = ET.fromstring(response.content)
                    
                    # Handle namespace
                    ns = {'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
                    
                    # Check if it's a sitemap index
                    sitemaps = root.findall('.//sm:sitemap/sm:loc', ns)
                    if sitemaps:
                        print(f"  → Found sitemap index with {len(sitemaps)} sitemaps")
                        # Parse each sitemap
                        for sitemap in sitemaps[:5]:  # Limit to first 5 sitemaps
                            try:
                                sub_response = requests.get(sitemap.text, timeout=10)
                                sub_root = ET.fromstring(sub_response.content)
                                urls = sub_root.findall('.//sm:url/sm:loc', ns)
                                for url in urls:
                                    clean_url = self.clean_url(url.text)
                                    if not self.should_skip_url(clean_url):
                                        found_urls.append(clean_url)
                            except:
                                continue
                    else:
                        # Regular sitemap
                        urls = root.findall('.//sm:url/sm:loc', ns)
                        print(f"  → Found {len(urls)} URLs in sitemap")
                        
                        for url_elem in urls:
                            url = url_elem.text
                            clean_url = self.clean_url(url)
                            if not self.should_skip_url(clean_url):
                                found_urls.append(clean_url)
                    
                    break  # Stop after first successful sitemap
                    
            except Exception as e:
                continue
        
        if not found_urls:
            print("  ✗ No sitemap found")
        else:
            print(f"  ✓ Extracted {len(found_urls)} URLs from sitemap")
        
        return found_urls
    
    def extract_all_links(self, url):
        """
        Extract all internal links from a page
        
        Args:
            url (str): URL to extract from
            
        Returns:
            dict: {url: context} where context is 'nav', 'footer', 'body', or 'homepage'
        """
        links = {}
        
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract navigation links (highest priority)
            nav_elements = soup.find_all(['nav', 'header'])
            for nav in nav_elements:
                for link in nav.find_all('a', href=True):
                    href = link.get('href')
                    if not href.startswith(('#', 'javascript:', 'mailto:', 'tel:')):
                        absolute_url = urljoin(url, href)
                        if urlparse(absolute_url).netloc == self.domain:
                            clean_url = self.clean_url(absolute_url)
                            if not self.should_skip_url(clean_url):
                                links[clean_url] = 'nav'
            
            # Extract footer links
            footer_elements = soup.find_all('footer')
            for footer in footer_elements:
                for link in footer.find_all('a', href=True):
                    href = link.get('href')
                    if not href.startswith(('#', 'javascript:', 'mailto:', 'tel:')):
                        absolute_url = urljoin(url, href)
                        if urlparse(absolute_url).netloc == self.domain:
                            clean_url = self.clean_url(absolute_url)
                            if not self.should_skip_url(clean_url) and clean_url not in links:
                                links[clean_url] = 'footer'
            
            # Extract ALL body links
            for link in soup.find_all('a', href=True):
                href = link.get('href')
                if not href.startswith(('#', 'javascript:', 'mailto:', 'tel:')):
                    absolute_url = urljoin(url, href)
                    if urlparse(absolute_url).netloc == self.domain:
                        clean_url = self.clean_url(absolute_url)
                        if not self.should_skip_url(clean_url) and clean_url not in links:
                            # Determine context
                            if url == self.base_url:
                                links[clean_url] = 'homepage'
                            else:
                                links[clean_url] = 'body'
            
        except Exception as e:
            print(f"  ⚠️  Error extracting links from {url}: {e}")
        
        return links
    
    def discover_important_pages(self):
        """
        Comprehensive crawl - extracts all linked pages (language-agnostic)
        Uses breadth-first search with depth limit
        """
        print("=" * 70)
        print("🧠 Smart Crawler - Comprehensive Link Discovery")
        print("=" * 70)
        print(f"Target: {self.base_url}")
        print(f"Max URLs: {self.max_urls}")
        print(f"Max Depth: {self.max_depth}")
        print("-" * 70)
        
        # Initialize with homepage
        self.discovered_urls[self.base_url] = 100
        self.url_depths[self.base_url] = 0
        
        # Queue for breadth-first crawl: (url, depth)
        to_crawl = deque([(self.base_url, 0)])
        crawled = set()
        
        # Strategy 1: Try sitemap first for comprehensive list
        sitemap_urls = self.parse_sitemap()
        for url in sitemap_urls:
            if url not in self.discovered_urls:
                depth = len(urlparse(url).path.strip('/').split('/'))
                priority = self.calculate_priority(url, 'sitemap', depth=1)
                self.discovered_urls[url] = priority
                self.url_depths[url] = 1
        
        print(f"\n📍 Crawling site structure (breadth-first)...")
        
        # Strategy 2: Breadth-first crawl
        while to_crawl and len(self.discovered_urls) < self.max_urls:
            current_url, current_depth = to_crawl.popleft()
            
            # Skip if already crawled or depth limit reached
            if current_url in crawled or current_depth > self.max_depth:
                continue
            
            crawled.add(current_url)
            print(f"  → Crawling [{current_depth}]: {current_url}")
            
            # Extract all links from current page
            page_links = self.extract_all_links(current_url)
            
            for url, context in page_links.items():
                if len(self.discovered_urls) >= self.max_urls:
                    break
                
                # Calculate priority
                priority = self.calculate_priority(url, context, depth=current_depth + 1)
                
                # Add to discovered URLs
                if url not in self.discovered_urls:
                    self.discovered_urls[url] = priority
                    self.url_depths[url] = current_depth + 1
                    
                    # Add to queue if within depth limit
                    if current_depth + 1 <= self.max_depth:
                        to_crawl.append((url, current_depth + 1))
        
        print(f"\n✅ Total unique URLs discovered: {len(self.discovered_urls)}")
        print(f"   Crawled {len(crawled)} pages to find them")
    
    def save_results(self):
        """Save discovered URLs to CSV, sorted by priority"""
        # Sort by priority (highest first)
        sorted_urls = sorted(
            self.discovered_urls.items(), 
            key=lambda x: x[1], 
            reverse=True
        )
        
        # Limit to max_urls
        sorted_urls = sorted_urls[:self.max_urls]
        
        with open(self.output_file, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['url', 'isUseful', 'priority'])
            
            for url, priority in sorted_urls:
                writer.writerow([url, '', priority])
        
        print(f"\n💾 Saved {len(sorted_urls)} URLs to: {self.output_file}")
        
        # Show top URLs
        print("\n🏆 Top 10 URLs by priority:")
        for i, (url, priority) in enumerate(sorted_urls[:10], 1):
            print(f"  {i}. [{priority:3d}] {url}")
    
    def crawl(self):
        """Main crawl method"""
        try:
            self.discover_important_pages()
            self.save_results()
            
            print("\n" + "=" * 70)
            print("✨ Smart crawling complete!")
            print("=" * 70)
            
        except KeyboardInterrupt:
            print("\n\n⚠️  Crawling interrupted by user!")
            if self.discovered_urls:
                print("Saving discovered URLs...")
                self.save_results()


def generate_filename(url):
    """Generate a meaningful filename from a URL"""
    parsed = urlparse(url)
    domain = parsed.netloc.replace('www.', '').replace('.', '_')
    domain = re.sub(r'[^\w\-]', '_', domain)
    return f"{domain}_urls.csv"


def crawl_single_site(url, max_urls=100):
    """Crawl a single site intelligently"""
    output_file = generate_filename(url)
    
    print(f"\n{'=' * 70}")
    print(f"Starting smart crawl for: {url}")
    print(f"Output file: {output_file}")
    print(f"{'=' * 70}")
    
    try:
        crawler = SmartCrawler(url, output_file, max_urls=max_urls)
        crawler.crawl()
        
        return {
            'url': url,
            'output_file': output_file,
            'count': len(crawler.discovered_urls),
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


def crawl_from_csv(csv_file, max_workers=3, max_urls=100):
    """Crawl multiple sites from CSV in parallel"""
    print("=" * 70)
    print("Smart Batch Crawler - Processing multiple sites")
    print("=" * 70)
    
    # Read URLs
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
    
    if not base_urls:
        print(f"\n❌ No valid URLs found in '{csv_file}'")
        return
    
    print(f"\nFound {len(base_urls)} URL(s) to crawl:")
    for idx, url in enumerate(base_urls, 1):
        print(f"  {idx}. {url} → {generate_filename(url)}")
    
    print(f"\nCrawling with {max_workers} parallel workers...")
    print(f"Max URLs per site: {max_urls}")
    print("-" * 70)
    
    # Crawl in parallel
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {
            executor.submit(crawl_single_site, url, max_urls): url 
            for url in base_urls
        }
        
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
    
    # Summary
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
    
    print(f"\n{'=' * 70}")
    print(f"Total: {len(successful)}/{len(results)} sites crawled successfully")
    print(f"{'=' * 70}")


def main():
    """Main function"""
    print("=" * 70)
    print("🧠 Smart Web Crawler - Comprehensive Link Discovery")
    print("=" * 70)
    print("\nUniversal Approach (works for any language/site):")
    print("  1. Parse sitemap.xml (if available)")
    print("  2. Breadth-first crawl from homepage")
    print("  3. Extract ALL internal links (no keyword assumptions)")
    print("  4. Score by depth and context (nav/footer/body)")
    print("  5. Crawl up to max depth and max URLs")
    print()
    print("Choose mode:")
    print("  1. Single URL (interactive)")
    print("  2. Batch mode (from base_urls.csv)")
    print()
    
    mode = input("Enter mode (1 or 2, default: 1): ").strip() or "1"
    
    max_urls_input = input("\nMax URLs per site (default: 100): ").strip()
    max_urls = int(max_urls_input) if max_urls_input.isdigit() else 100
    
    if mode == "2":
        # Batch mode
        csv_file = input("\nEnter CSV file (default: base_urls.csv): ").strip() or "base_urls.csv"
        max_workers = input("Max parallel crawlers (default: 3): ").strip()
        max_workers = int(max_workers) if max_workers.isdigit() else 3
        
        crawl_from_csv(csv_file, max_workers=max_workers, max_urls=max_urls)
    else:
        # Single URL mode
        base_url = input("\nEnter the base URL (e.g., https://techstrata.com): ").strip()
        
        if not base_url.startswith(('http://', 'https://')):
            print("Error: URL must start with http:// or https://")
            return
        
        output_file = input("Enter output filename (default: urls.csv): ").strip() or "urls.csv"
        
        print()
        
        crawler = SmartCrawler(base_url, output_file, max_urls=max_urls)
        crawler.crawl()
    
    print("\n✨ Done!")


if __name__ == "__main__":
    main()


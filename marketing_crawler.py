#!/usr/bin/env python3
"""
Marketing Crawler - Professional End-to-End Solution
Supports multiple workflows with parallel processing
"""

import os
import sys
import yaml
import argparse
from datetime import datetime
from pathlib import Path

# Import our existing modules
from smart_crawler import SmartCrawler, crawl_from_csv
from content_crawler import ContentExtractor
from value_extraction import main as extract_values
from label_urls import main as label_urls_main


class MarketingCrawler:
    def __init__(self, config_file="config.yaml"):
        """Initialize with configuration"""
        self.load_config(config_file)
        self.setup_directories()
    
    def load_config(self, config_file):
        """Load configuration from YAML"""
        if os.path.exists(config_file):
            with open(config_file, 'r') as f:
                self.config = yaml.safe_load(f)
        else:
            print(f"⚠️  Config file '{config_file}' not found. Using defaults.")
            self.config = self.get_default_config()
    
    def get_default_config(self):
        """Return default configuration"""
        return {
            'crawling': {
                'max_urls_per_site': 200,
                'max_depth': 3,
                'timeout_seconds': 30,
                'delay_between_requests': 1
            },
            'content_extraction': {
                'timeout_seconds': 30,
                'max_retries': 5,
                'output_format': 'all',
                'parallel_workers': 5
            },
            'value_extraction': {
                'model': 'gpt-4o-mini',
                'temperature': 0.2,
                'parallel_workers': 3
            },
            'output': {
                'urls_dir': '.',
                'content_dir': 'extracted_content',
                'final_data': 'company_data.json',
                'logs_dir': 'logs',
                'progress_dir': 'progress'
            },
            'url_labeling': {
                'enabled': True,
                'batch_size': 50
            }
        }
    
    def setup_directories(self):
        """Create necessary directories"""
        dirs = [
            self.config['output']['content_dir'],
            self.config['output']['logs_dir'],
            self.config['output']['progress_dir']
        ]
        for dir_path in dirs:
            os.makedirs(dir_path, exist_ok=True)
    
    def crawl_single_site(self, url):
        """
        Workflow 1: Crawl a single website
        """
        print("\n" + "="*70)
        print("🌐 WORKFLOW: Single Site Crawl")
        print("="*70)
        print(f"URL: {url}")
        print(f"Max URLs: {self.config['crawling']['max_urls_per_site']}")
        print("-"*70)
        
        # Generate output filename
        from urllib.parse import urlparse
        domain = urlparse(url).netloc.replace('www.', '').replace('.', '_')
        output_file = f"{domain}_urls.csv"
        
        # Step 1: Crawl URLs
        print("\n📍 Step 1/4: Crawling website for URLs...")
        crawler = SmartCrawler(
            base_url=url,
            output_file=output_file,
            max_urls=self.config['crawling']['max_urls_per_site']
        )
        crawler.crawl()
        
        # Step 2: Label URLs (if enabled)
        if self.config['url_labeling']['enabled']:
            print("\n🏷️  Step 2/4: Labeling URLs with AI...")
            # Call label_urls
            os.system(f"python label_urls.py {output_file}")
        else:
            print("\n⏭️  Step 2/4: URL labeling disabled (skipped)")
        
        # Step 3: Extract content
        print("\n📄 Step 3/4: Extracting content from useful pages...")
        self.extract_content_from_csv(output_file)
        
        # Step 4: Extract values with AI
        print("\n🤖 Step 4/4: Extracting company data with AI...")
        extract_values()
        
        print("\n" + "="*70)
        print("✅ COMPLETE! Single site workflow finished.")
        print("="*70)
        
        return output_file
    
    def crawl_multiple_sites(self, csv_file):
        """
        Workflow 2: Crawl multiple websites from CSV
        """
        print("\n" + "="*70)
        print("🌐 WORKFLOW: Multiple Sites Crawl")
        print("="*70)
        print(f"Input CSV: {csv_file}")
        print(f"Parallel workers: {self.config['content_extraction']['parallel_workers']}")
        print("-"*70)
        
        # Step 1: Crawl all sites
        print("\n📍 Step 1/4: Crawling all websites for URLs...")
        crawl_from_csv(
            csv_file=csv_file,
            max_workers=self.config['content_extraction']['parallel_workers'],
            max_urls=self.config['crawling']['max_urls_per_site']
        )
        
        print("\n" + "="*70)
        print("✅ COMPLETE! Multiple sites crawled.")
        print("Next steps:")
        print("  - Review generated CSV files")
        print("  - Label URLs: python label_urls.py <csv_file>")
        print("  - Extract content: python marketing_crawler.py extract-content <csv_file>")
        print("  - Extract values: python marketing_crawler.py extract-values")
        print("="*70)
    
    def extract_content_from_csv(self, csv_file):
        """
        Extract content from URLs in CSV file
        """
        print("\n" + "="*70)
        print("📄 Content Extraction")
        print("="*70)
        
        import csv
        
        # Read useful URLs
        useful_urls = []
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get('isUseful', '').strip().lower() == 'true':
                    useful_urls.append(row['url'])
        
        if not useful_urls:
            print("❌ No URLs marked as useful. Please label URLs first!")
            return
        
        print(f"Found {len(useful_urls)} useful URLs")
        print(f"Parallel workers: {self.config['content_extraction']['parallel_workers']}")
        print(f"Output: {self.config['output']['content_dir']}/")
        print("-"*70)
        
        # Extract with parallel processing
        from concurrent.futures import ThreadPoolExecutor, as_completed
        import time
        
        extractor = ContentExtractor(
            output_dir=self.config['output']['content_dir'],
            max_retries=self.config['content_extraction']['max_retries'],
            timeout=self.config['content_extraction']['timeout_seconds']
        )
        
        successful = 0
        failed = 0
        
        with ThreadPoolExecutor(max_workers=self.config['content_extraction']['parallel_workers']) as executor:
            futures = {
                executor.submit(extractor.extract_clean_content, url): (idx, url)
                for idx, url in enumerate(useful_urls, 1)
            }
            
            for future in as_completed(futures):
                idx, url = futures[future]
                print(f"\n[{idx}/{len(useful_urls)}] {url}")
                
                try:
                    data = future.result()
                    
                    if data['extraction_successful']:
                        successful += 1
                        
                        # Save files
                        from urllib.parse import urlparse
                        import re
                        parsed = urlparse(url)
                        filename_base = parsed.path.strip('/').replace('/', '_') or 'homepage'
                        filename_base = re.sub(r'[^\w\-_]', '_', filename_base)
                        filename_base = f"{idx}_{filename_base}"
                        
                        output_format = self.config['content_extraction']['output_format']
                        if output_format in ['json', 'all']:
                            extractor.save_as_json(data, f"{filename_base}.json")
                        if output_format in ['markdown', 'all']:
                            extractor.save_as_markdown(data, f"{filename_base}.md")
                        if output_format in ['text', 'all']:
                            extractor.save_as_text(data, f"{filename_base}.txt")
                    else:
                        failed += 1
                        
                except Exception as e:
                    print(f"  ✗ Error: {e}")
                    failed += 1
                
                time.sleep(self.config['crawling']['delay_between_requests'])
        
        print("\n" + "="*70)
        print("SUMMARY:")
        print(f"  ✅ Successful: {successful}")
        print(f"  ❌ Failed: {failed}")
        print("="*70)


def print_banner():
    """Print banner"""
    print("""
╔═══════════════════════════════════════════════════════════════╗
║                  MARKETING CRAWLER v2.0                       ║
║              Professional End-to-End Solution                 ║
╚═══════════════════════════════════════════════════════════════╝
    """)


def main():
    """Main CLI"""
    print_banner()
    
    parser = argparse.ArgumentParser(
        description="Professional marketing website crawler with AI extraction",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Single website (complete workflow)
  python marketing_crawler.py single https://example.com
  
  # Multiple websites from CSV
  python marketing_crawler.py batch urls.csv
  
  # Just crawl URLs (step 1 only)
  python marketing_crawler.py crawl-single https://example.com
  python marketing_crawler.py crawl-batch urls.csv
  
  # Just extract content (step 3 only)
  python marketing_crawler.py extract-content example_urls.csv
  
  # Just extract values with AI (step 4 only)
  python marketing_crawler.py extract-values
  
  # Label URLs with AI (step 2 only)
  python marketing_crawler.py label-urls example_urls.csv
        """
    )
    
    parser.add_argument('command', choices=[
        'single', 'batch', 
        'crawl-single', 'crawl-batch',
        'label-urls', 'extract-content', 'extract-values'
    ], help='Command to run')
    
    parser.add_argument('target', nargs='?', help='URL or CSV file')
    parser.add_argument('--config', default='config.yaml', help='Config file (default: config.yaml)')
    parser.add_argument('--max-urls', type=int, help='Override max URLs per site')
    parser.add_argument('--workers', type=int, help='Override parallel workers')
    
    args = parser.parse_args()
    
    # Initialize crawler
    crawler = MarketingCrawler(config_file=args.config)
    
    # Override config with CLI args if provided
    if args.max_urls:
        crawler.config['crawling']['max_urls_per_site'] = args.max_urls
    if args.workers:
        crawler.config['content_extraction']['parallel_workers'] = args.workers
        crawler.config['value_extraction']['parallel_workers'] = args.workers
    
    # Execute command
    try:
        if args.command == 'single':
            if not args.target:
                print("❌ Error: URL required")
                print("Usage: python marketing_crawler.py single https://example.com")
                return
            crawler.crawl_single_site(args.target)
        
        elif args.command == 'batch':
            if not args.target:
                print("❌ Error: CSV file required")
                print("Usage: python marketing_crawler.py batch urls.csv")
                return
            crawler.crawl_multiple_sites(args.target)
        
        elif args.command == 'crawl-single':
            if not args.target:
                print("❌ Error: URL required")
                return
            from smart_crawler import SmartCrawler
            from urllib.parse import urlparse
            domain = urlparse(args.target).netloc.replace('www.', '').replace('.', '_')
            output_file = f"{domain}_urls.csv"
            crawler_obj = SmartCrawler(
                base_url=args.target,
                output_file=output_file,
                max_urls=crawler.config['crawling']['max_urls_per_site']
            )
            crawler_obj.crawl()
        
        elif args.command == 'crawl-batch':
            if not args.target:
                print("❌ Error: CSV file required")
                return
            crawl_from_csv(
                csv_file=args.target,
                max_workers=crawler.config['content_extraction']['parallel_workers'],
                max_urls=crawler.config['crawling']['max_urls_per_site']
            )
        
        elif args.command == 'label-urls':
            if not args.target:
                print("❌ Error: CSV file required")
                return
            os.system(f"python label_urls.py {args.target}")
        
        elif args.command == 'extract-content':
            if not args.target:
                print("❌ Error: CSV file required")
                return
            crawler.extract_content_from_csv(args.target)
        
        elif args.command == 'extract-values':
            extract_values()
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()


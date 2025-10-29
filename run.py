#!/usr/bin/env python3
"""
Marketing Crawler - Professional End-to-End Solution
Clean project structure, batch processing, fully automated
"""

import os
import sys
import json
import yaml
import argparse
import csv
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse
import re

# Import existing modules
from smart_crawler import SmartCrawler
from content_crawler import ContentExtractor
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()


class ProjectManager:
    """Manages project folders for each website"""
    
    def __init__(self, base_dir="projects"):
        self.base_dir = base_dir
        os.makedirs(base_dir, exist_ok=True)
    
    def get_project_name(self, url):
        """Generate clean project name from URL"""
        domain = urlparse(url).netloc.replace('www.', '')
        # Clean domain name
        project_name = re.sub(r'[^\w\-]', '_', domain)
        return project_name
    
    def create_project(self, url):
        """Create project folder structure for a website"""
        project_name = self.get_project_name(url)
        project_dir = os.path.join(self.base_dir, project_name)
        
        # Create folder structure
        folders = {
            'root': project_dir,
            'content': os.path.join(project_dir, '2_content'),
            'logs': os.path.join(project_dir, 'logs')
        }
        
        for folder in folders.values():
            os.makedirs(folder, exist_ok=True)
        
        # Save project metadata
        metadata = {
            'url': url,
            'project_name': project_name,
            'created_at': datetime.now().isoformat(),
            'status': 'created'
        }
        
        with open(os.path.join(project_dir, 'project.json'), 'w') as f:
            json.dump(metadata, f, indent=2)
        
        return project_dir, folders
    
    def get_project_dir(self, url):
        """Get existing project directory"""
        project_name = self.get_project_name(url)
        return os.path.join(self.base_dir, project_name)
    
    def list_projects(self):
        """List all projects"""
        projects = []
        if not os.path.exists(self.base_dir):
            return projects
        
        for item in os.listdir(self.base_dir):
            project_dir = os.path.join(self.base_dir, item)
            if os.path.isdir(project_dir):
                metadata_file = os.path.join(project_dir, 'project.json')
                if os.path.exists(metadata_file):
                    with open(metadata_file, 'r') as f:
                        metadata = json.load(f)
                    projects.append(metadata)
        
        return projects


class WorkflowEngine:
    """Manages end-to-end workflow for each website"""
    
    def __init__(self, config_file="config.yaml"):
        self.load_config(config_file)
        self.project_manager = ProjectManager()
    
    def load_config(self, config_file):
        """Load configuration"""
        if os.path.exists(config_file):
            with open(config_file, 'r') as f:
                self.config = yaml.safe_load(f)
        else:
            self.config = {
                'crawling': {'max_urls_per_site': 200, 'max_depth': 3},
                'content_extraction': {'parallel_workers': 5, 'max_retries': 5, 'timeout_seconds': 30},
                'value_extraction': {'model': 'gpt-4o-mini', 'temperature': 0.2}
            }
    
    def run_single_site(self, url):
        """Complete workflow for single site"""
        print("\n" + "="*80)
        print(f"🚀 PROCESSING: {url}")
        print("="*80)
        
        # Step 0: Create project structure
        print("\n📁 Step 1/5: Creating project structure...")
        project_dir, folders = self.project_manager.create_project(url)
        print(f"   Project: {project_dir}")
        
        # Step 1: Crawl URLs
        print("\n🕷️  Step 2/5: Crawling website for URLs...")
        urls_file = os.path.join(project_dir, '1_urls.csv')
        crawler = SmartCrawler(
            base_url=url,
            output_file=urls_file,
            max_urls=self.config['crawling']['max_urls_per_site']
        )
        crawler.crawl()
        print(f"   ✓ URLs saved to: {urls_file}")
        
        # Step 2: Label URLs with AI
        print("\n🏷️  Step 3/5: Labeling URLs (which are useful)...")
        self.label_urls(urls_file, project_dir)
        
        # Step 3: Extract content from useful URLs
        print("\n📄 Step 4/5: Extracting content from useful pages...")
        self.extract_content(urls_file, folders['content'], project_dir)
        
        # Step 4: Extract company data with AI
        print("\n🤖 Step 5/5: Extracting company data with AI...")
        self.extract_values(folders['content'], project_dir)
        
        # Update project status
        self.update_project_status(project_dir, 'completed')
        
        print("\n" + "="*80)
        print(f"✅ COMPLETED: {url}")
        print(f"📂 Results in: {project_dir}/")
        print("="*80)
        
        return project_dir
    
    def run_batch(self, csv_file):
        """Process multiple sites from CSV"""
        print("\n" + "="*80)
        print(f"🚀 BATCH PROCESSING from {csv_file}")
        print("="*80)
        
        # Read URLs
        urls = []
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                url = row.get('url', '').strip()
                if url and url.startswith(('http://', 'https://')):
                    urls.append(url)
        
        print(f"\nFound {len(urls)} website(s) to process")
        print("-"*80)
        
        results = []
        for idx, url in enumerate(urls, 1):
            print(f"\n[{idx}/{len(urls)}] Processing: {url}")
            try:
                project_dir = self.run_single_site(url)
                results.append({'url': url, 'status': 'success', 'project_dir': project_dir})
            except Exception as e:
                print(f"   ❌ Error: {e}")
                results.append({'url': url, 'status': 'failed', 'error': str(e)})
        
        # Summary
        print("\n" + "="*80)
        print("📊 BATCH SUMMARY")
        print("="*80)
        successful = [r for r in results if r['status'] == 'success']
        failed = [r for r in results if r['status'] == 'failed']
        
        print(f"\n✅ Successful: {len(successful)}/{len(urls)}")
        for r in successful:
            print(f"   • {r['url']}")
            print(f"     → {r['project_dir']}/")
        
        if failed:
            print(f"\n❌ Failed: {len(failed)}/{len(urls)}")
            for r in failed:
                print(f"   • {r['url']}: {r['error']}")
        
        print("\n" + "="*80)
        
        return results
    
    def label_urls(self, urls_file, project_dir):
        """Label URLs with AI using the original label_urls.py logic"""
        from label_urls import label_urls_with_openai
        
        # Use the original labeling function that already has all the correct logic
        label_urls_with_openai(
            input_csv=urls_file,
            output_csv=urls_file,
            api_key=os.getenv('OPENAI_API_KEY')
        )
    
    def extract_content(self, urls_file, content_dir, project_dir, file_queue=None, job_id=None, job_cancellation=None, tracker=None):
        """Extract content from useful URLs (Producer for pipeline)"""
        # Read useful URLs
        useful_urls = []
        with open(urls_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get('isUseful', '').strip().lower() == 'true':
                    useful_urls.append(row['url'])
        
        if not useful_urls:
            msg = "   ⚠️  No useful URLs to extract"
            if tracker:
                tracker.log(msg)
            else:
                print(msg)
            if file_queue:
                file_queue.put(None)  # Signal completion
            return
        
        msg = f"   Extracting content from {len(useful_urls)} useful URLs..."
        if tracker:
            tracker.log(msg)
        else:
            print(msg)
        
        # Extract with retry logic
        extractor = ContentExtractor(
            output_dir=content_dir,
            max_retries=self.config['content_extraction']['max_retries'],
            timeout=self.config['content_extraction']['timeout_seconds']
        )
        
        successful = 0
        failed_urls = []
        
        for idx, url in enumerate(useful_urls, 1):
            # Check for cancellation
            if job_id and job_cancellation and job_cancellation.get(job_id):
                msg = "   ⚠️  Content extraction cancelled by user"
                if tracker:
                    tracker.log(msg)
                else:
                    print(msg)
                if file_queue:
                    file_queue.put(None)  # Signal completion
                return
            
            msg = f"   [{idx}/{len(useful_urls)}] {url}"
            if tracker:
                tracker.log(msg)
            else:
                print(msg)
            
            data = extractor.extract_clean_content(url)
            
            if data['extraction_successful']:
                successful += 1
                # Save as markdown
                filename = f"{idx}_{self.sanitize_filename(url)}.md"
                filepath = extractor.save_as_markdown(data, filename)
                
                # If queue provided, put file path for parallel processing
                if file_queue:
                    file_queue.put(filepath)
            else:
                failed_urls.append({'url': url, 'error': data.get('error', 'Unknown')})
        
        msg = f"   ✓ Extracted {successful}/{len(useful_urls)} pages"
        if tracker:
            tracker.log(msg)
        else:
            print(msg)
        
        # Signal completion to consumer
        if file_queue:
            file_queue.put(None)
        
        # Save failed URLs
        if failed_urls:
            failed_file = os.path.join(project_dir, 'logs', 'failed_content_extraction.json')
            with open(failed_file, 'w') as f:
                json.dump(failed_urls, f, indent=2)
    
    def extract_values(self, content_dir, project_dir):
        """Extract company data with AI using original value_extraction.py logic"""
        from value_extraction import (
            get_empty_structure,
            get_markdown_files,
            extract_value_from_file,
            save_progress,
            save_output
        )
        
        # Get markdown files using original logic
        md_files = get_markdown_files(content_dir)
        
        if not md_files:
            print("   ⚠️  No content files to process")
            return
        
        print(f"   Processing {len(md_files)} content files with AI...")
        
        # Initialize OpenAI client
        client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
        
        # Set up file paths
        log_file = os.path.join(project_dir, 'logs', 'openai_requests.json')
        progress_file = os.path.join(project_dir, 'logs', 'extraction_progress.json')
        output_file = os.path.join(project_dir, '3_company_data.json')
        
        # Initialize with empty structure
        accumulated_data = get_empty_structure()
        
        # Process each file using original logic
        for idx, filepath in enumerate(md_files, 1):
            filename = os.path.basename(filepath)
            print(f"   [{idx}/{len(md_files)}] {filename}")
            
            # Use original extract_value_from_file function
            accumulated_data = extract_value_from_file(
                client=client,
                filepath=filepath,
                current_data=accumulated_data,
                log_file=log_file
            )
            
            # Save progress after each file
            save_progress(accumulated_data, progress_file, filename)
        
        # Save final output using original function
        save_output(accumulated_data, output_file)
        
        print(f"   ✓ Company data saved to: 3_company_data.json")
    
    def extract_values_from_queue(self, file_queue, project_dir, job_id=None, job_cancellation=None, tracker=None):
        """Extract company data from files as they arrive (Consumer for pipeline)"""
        from value_extraction import (
            get_empty_structure,
            extract_value_from_file,
            save_progress,
            save_output
        )
        
        msg = "   🤖 AI Value Extraction running in parallel..."
        if tracker:
            tracker.log(msg)
        else:
            print(msg)
        
        # Initialize OpenAI client
        client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
        
        # Set up file paths
        log_file = os.path.join(project_dir, 'logs', 'openai_requests.json')
        progress_file = os.path.join(project_dir, 'logs', 'extraction_progress.json')
        output_file = os.path.join(project_dir, '3_company_data.json')
        
        # Initialize with empty structure
        accumulated_data = get_empty_structure()
        
        file_count = 0
        
        # Process files as they arrive
        while True:
            # Check for cancellation
            if job_id and job_cancellation and job_cancellation.get(job_id):
                msg = "   ⚠️  AI processing cancelled by user"
                if tracker:
                    tracker.log(msg)
                else:
                    print(msg)
                # Drain the queue
                while not file_queue.empty():
                    try:
                        file_queue.get_nowait()
                        file_queue.task_done()
                    except:
                        break
                # Save partial results
                if file_count > 0:
                    save_output(accumulated_data, output_file)
                    msg = f"   ✓ Partial results saved ({file_count} files processed)"
                    if tracker:
                        tracker.log(msg)
                    else:
                        print(msg)
                return
            
            filepath = file_queue.get()
            
            # None signals completion
            if filepath is None:
                break
            
            file_count += 1
            filename = os.path.basename(filepath)
            msg = f"   [{file_count}] Processing {filename}"
            if tracker:
                tracker.log(msg)
            else:
                print(msg)
            
            # Use original extract_value_from_file function
            accumulated_data = extract_value_from_file(
                client=client,
                filepath=filepath,
                current_data=accumulated_data,
                log_file=log_file
            )
            
            # Save progress after each file
            save_progress(accumulated_data, progress_file, filename)
            
            file_queue.task_done()
        
        # Save final output
        save_output(accumulated_data, output_file)
        msg = f"   ✓ Processed {file_count} files - Company data saved"
        if tracker:
            tracker.log(msg)
        else:
            print(msg)
    
    def sanitize_filename(self, url):
        """Create safe filename from URL"""
        path = urlparse(url).path
        name = path.strip('/').replace('/', '_') or 'homepage'
        return re.sub(r'[^\w\-]', '_', name)[:100]
    
    def get_company_schema(self):
        """Get company data schema"""
        return {
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
                                "other": {"type": "array", "items": {"type": "string"}}
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
                    "required": ["company_name", "company_email", "company_location", "company_phone", 
                               "company_industry_type", "company_social_links", "description", "company_persons"],
                    "additionalProperties": False
                }
            }
        }
    
    def get_empty_company_structure(self):
        """Get empty company data structure"""
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
    
    def update_project_status(self, project_dir, status):
        """Update project metadata"""
        metadata_file = os.path.join(project_dir, 'project.json')
        if os.path.exists(metadata_file):
            with open(metadata_file, 'r') as f:
                metadata = json.load(f)
            metadata['status'] = status
            metadata['updated_at'] = datetime.now().isoformat()
            with open(metadata_file, 'w') as f:
                json.dump(metadata, f, indent=2)


def main():
    """Main CLI"""
    print("""
╔══════════════════════════════════════════════════════════════╗
║           MARKETING CRAWLER - End-to-End Solution            ║
╚══════════════════════════════════════════════════════════════╝
""")
    
    parser = argparse.ArgumentParser(
        description="Professional marketing crawler with clean project structure",
        epilog="""
Examples:
  # Process single website (complete workflow)
  python run.py single https://example.com
  
  # Process multiple websites from CSV
  python run.py batch websites.csv
  
  # List all projects
  python run.py list
        """
    )
    
    parser.add_argument('command', choices=['single', 'batch', 'list'], help='Command to run')
    parser.add_argument('target', nargs='?', help='URL or CSV file')
    parser.add_argument('--config', default='config.yaml', help='Config file')
    
    args = parser.parse_args()
    
    # Initialize workflow engine
    engine = WorkflowEngine(config_file=args.config)
    
    try:
        if args.command == 'single':
            if not args.target:
                print("❌ Error: URL required")
                print("Usage: python run.py single https://example.com")
                return
            engine.run_single_site(args.target)
        
        elif args.command == 'batch':
            if not args.target:
                print("❌ Error: CSV file required")
                print("Usage: python run.py batch websites.csv")
                return
            engine.run_batch(args.target)
        
        elif args.command == 'list':
            projects = engine.project_manager.list_projects()
            if not projects:
                print("No projects found.")
            else:
                print(f"\nFound {len(projects)} project(s):\n")
                for p in projects:
                    print(f"  • {p['project_name']}")
                    print(f"    URL: {p['url']}")
                    print(f"    Status: {p.get('status', 'unknown')}")
                    print(f"    Created: {p['created_at']}")
                    print()
    
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()


#!/usr/bin/env python3
"""
Marketing Crawler - Web Dashboard
Modern UI for managing the entire pipeline
"""

from flask import Flask, render_template, request, jsonify, send_from_directory
from flask_socketio import SocketIO, emit
import os
import json
import yaml
import threading
from datetime import datetime
from pathlib import Path
from run import WorkflowEngine, ProjectManager

app = Flask(__name__)
app.config['SECRET_KEY'] = 'marketing-crawler-secret-key'
socketio = SocketIO(app, cors_allowed_origins="*")

# Global state
workflow_engine = WorkflowEngine()
project_manager = ProjectManager()
active_jobs = {}  # {job_id: tracker object}
job_cancellation = {}  # {job_id: bool} - True means "please stop"


class ProgressTracker:
    """Track progress and emit to UI"""
    
    def __init__(self, job_id):
        self.job_id = job_id
        self.current_step = 0
        self.total_steps = 5
        self.status = 'running'
        self.logs = []
    
    def log(self, message, level='info'):
        """Add log message"""
        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'message': message,
            'level': level
        }
        self.logs.append(log_entry)
        
        # Emit to UI
        socketio.emit('log', {
            'job_id': self.job_id,
            'log': log_entry
        })
    
    def update_step(self, step, total=None):
        """Update progress step"""
        self.current_step = step
        if total:
            self.total_steps = total
        
        # Emit to UI
        socketio.emit('progress', {
            'job_id': self.job_id,
            'step': step,
            'total': self.total_steps,
            'percentage': int((step / self.total_steps) * 100)
        })
    
    def complete(self, success=True):
        """Mark as complete"""
        self.status = 'completed' if success else 'failed'
        
        socketio.emit('job_complete', {
            'job_id': self.job_id,
            'status': self.status
        })


def run_pipeline_async(job_id, url, config):
    """Run pipeline in background thread (persists across page refreshes)"""
    import csv
    import traceback
    from queue import Queue
    from threading import Thread
    
    tracker = ProgressTracker(job_id)
    active_jobs[job_id] = tracker
    job_cancellation[job_id] = False  # Not cancelled initially
    
    try:
        tracker.log(f"🚀 Starting pipeline for: {url}")
        tracker.update_step(0, total=5)
        
        # Check for cancellation
        if job_cancellation.get(job_id):
            tracker.log("❌ Job cancelled by user", 'error')
            tracker.complete(success=False)
            return
        
        # Create project
        tracker.update_step(1)
        tracker.log("📁 Creating project structure...")
        project_dir, folders = project_manager.create_project(url)
        tracker.log(f"   Project folder: {project_dir}")
        
        # Step 1: Crawl URLs
        tracker.update_step(2)
        tracker.log("🕷️  Step 1/4: Crawling website for URLs...")
        
        # Check cancellation
        if job_cancellation.get(job_id):
            tracker.log("❌ Job cancelled by user", 'error')
            tracker.complete(success=False)
            return
        
        from smart_crawler import SmartCrawler
        urls_file = os.path.join(project_dir, '1_urls.csv')
        crawler = SmartCrawler(
            base_url=url,
            output_file=urls_file,
            max_urls=config['crawling']['max_urls_per_site']
        )
        crawler.crawl()
        tracker.log(f"   ✓ Found {len(crawler.discovered_urls)} URLs")
        
        # Step 2: Label URLs
        tracker.update_step(3)
        tracker.log("🏷️  Step 2/4: Labeling URLs with AI...")
        
        # Check cancellation
        if job_cancellation.get(job_id):
            tracker.log("❌ Job cancelled by user", 'error')
            tracker.complete(success=False)
            return
        
        # Read URLs
        with open(urls_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            urls_data = list(reader)
        
        if urls_data:
            tracker.log(f"   Processing {len(urls_data)} URLs...")
            workflow_engine.label_urls(urls_file, project_dir)
            
            # Count useful URLs after labeling
            with open(urls_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                useful = sum(1 for row in reader if row.get('isUseful', '').strip().lower() == 'true')
            
            if useful > 0:
                tracker.log(f"   ✓ Found {useful} useful URLs out of {len(urls_data)}")
            else:
                tracker.log(f"   ⚠️  No useful URLs found (all marked as not useful)")
        else:
            tracker.log("   ⚠️  No URLs to label")
        
        # Steps 3 & 4: Run in PARALLEL (Pipeline approach)
        tracker.update_step(4)
        tracker.log("📄 Step 3 & 4: Extracting content + AI processing (PARALLEL)...")
        
        # Count useful URLs first
        with open(urls_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            useful_urls = [row['url'] for row in reader if row.get('isUseful', '').strip().lower() == 'true']
        
        if useful_urls:
            tracker.log(f"   🔄 Pipeline mode: Extract → Process simultaneously")
            tracker.log(f"   📄 Content Extraction: {len(useful_urls)} pages")
            tracker.log(f"   🤖 AI Processing: As files arrive...")
            
            # Create queue for producer-consumer pattern
            file_queue = Queue()
            
            # Start AI consumer in separate thread (processes files as they arrive)
            consumer_thread = Thread(
                target=workflow_engine.extract_values_from_queue,
                args=(file_queue, project_dir)
            )
            consumer_thread.daemon = True
            consumer_thread.start()
            
            # Run content extractor (producer) - feeds files to queue
            workflow_engine.extract_content(urls_file, folders['content'], project_dir, file_queue)
            
            # Wait for AI processing to complete
            consumer_thread.join()
            
            tracker.update_step(5)
            tracker.log(f"   ✓ Pipeline complete! Content extracted & AI processed in parallel")
        else:
            tracker.log("   ⚠️  No useful URLs to extract content from")
        
        # Update project status
        workflow_engine.update_project_status(project_dir, 'completed')
        
        tracker.log("✅ Pipeline completed successfully!", 'success')
        tracker.log(f"📂 Results saved to: {project_dir}")
        tracker.complete(success=True)
        
    except Exception as e:
        error_msg = str(e)
        tracker.log(f"❌ Error: {error_msg}", 'error')
        tracker.log(f"   Traceback: {traceback.format_exc()}", 'error')
        tracker.complete(success=False)
    
    finally:
        # Cleanup
        if job_id in job_cancellation:
            del job_cancellation[job_id]


# Routes
@app.route('/')
def index():
    """Main dashboard"""
    return render_template('index.html')


@app.route('/api/config', methods=['GET'])
def get_config():
    """Get current configuration"""
    return jsonify(workflow_engine.config)


@app.route('/api/config', methods=['POST'])
def update_config():
    """Update configuration"""
    new_config = request.json
    workflow_engine.config.update(new_config)
    
    # Save to file
    with open('config.yaml', 'w') as f:
        yaml.dump(workflow_engine.config, f)
    
    return jsonify({'success': True})


@app.route('/api/projects', methods=['GET'])
def get_projects():
    """List all projects"""
    projects = project_manager.list_projects()
    
    # Add file sizes and stats
    for project in projects:
        project_dir = project_manager.get_project_dir(project['url'])
        
        # Check what files exist
        project['has_urls'] = os.path.exists(os.path.join(project_dir, '1_urls.csv'))
        project['has_content'] = os.path.exists(os.path.join(project_dir, '2_content'))
        project['has_data'] = os.path.exists(os.path.join(project_dir, '3_company_data.json'))
        
        # Count files
        if project['has_content']:
            content_dir = os.path.join(project_dir, '2_content')
            project['content_count'] = len([f for f in os.listdir(content_dir) if f.endswith('.md')])
        else:
            project['content_count'] = 0
    
    return jsonify(projects)


@app.route('/api/projects/<project_name>', methods=['GET'])
def get_project_details(project_name):
    """Get detailed project information"""
    project_dir = os.path.join('projects', project_name)
    
    if not os.path.exists(project_dir):
        return jsonify({'error': 'Project not found'}), 404
    
    # Read project metadata
    metadata_file = os.path.join(project_dir, 'project.json')
    if os.path.exists(metadata_file):
        with open(metadata_file, 'r') as f:
            metadata = json.load(f)
    else:
        metadata = {}
    
    # Read company data if exists
    data_file = os.path.join(project_dir, '3_company_data.json')
    if os.path.exists(data_file):
        with open(data_file, 'r') as f:
            company_data = json.load(f)
    else:
        company_data = None
    
    # Read URLs
    urls_file = os.path.join(project_dir, '1_urls.csv')
    urls_data = []
    if os.path.exists(urls_file):
        import csv
        with open(urls_file, 'r') as f:
            reader = csv.DictReader(f)
            urls_data = list(reader)
    
    # List content files
    content_dir = os.path.join(project_dir, '2_content')
    content_files = []
    if os.path.exists(content_dir):
        content_files = [f for f in os.listdir(content_dir) if f.endswith('.md')]
    
    # Read logs
    log_file = os.path.join(project_dir, 'logs', 'openai_requests.json')
    logs = []
    if os.path.exists(log_file):
        with open(log_file, 'r') as f:
            logs = json.load(f)
    
    return jsonify({
        'metadata': metadata,
        'company_data': company_data,
        'urls': urls_data,
        'content_files': content_files,
        'logs': logs
    })


@app.route('/api/projects/<project_name>', methods=['DELETE'])
def delete_project(project_name):
    """Delete a project"""
    import shutil
    project_dir = os.path.join('projects', project_name)
    
    if os.path.exists(project_dir):
        shutil.rmtree(project_dir)
        return jsonify({'success': True})
    
    return jsonify({'error': 'Project not found'}), 404


@app.route('/api/start-single', methods=['POST'])
def start_single_pipeline():
    """Start pipeline for single website"""
    data = request.json
    url = data.get('url')
    
    if not url:
        return jsonify({'error': 'URL required'}), 400
    
    # Generate job ID
    job_id = f"job_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # Start in background thread
    thread = threading.Thread(
        target=run_pipeline_async,
        args=(job_id, url, workflow_engine.config)
    )
    thread.daemon = True
    thread.start()
    
    return jsonify({
        'job_id': job_id,
        'status': 'started'
    })


@app.route('/api/start-batch', methods=['POST'])
def start_batch_pipeline():
    """Start pipeline for multiple websites"""
    data = request.json
    urls = data.get('urls', [])
    
    if not urls:
        return jsonify({'error': 'URLs required'}), 400
    
    job_ids = []
    
    for url in urls:
        job_id = f"job_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{len(job_ids)}"
        
        thread = threading.Thread(
            target=run_pipeline_async,
            args=(job_id, url, workflow_engine.config)
        )
        thread.daemon = True
        thread.start()
        
        job_ids.append(job_id)
    
    return jsonify({
        'job_ids': job_ids,
        'status': 'started',
        'count': len(urls)
    })


@app.route('/api/jobs', methods=['GET'])
def get_jobs():
    """Get all active jobs (persists across page refreshes)"""
    jobs_data = {}
    
    for job_id, tracker in active_jobs.items():
        jobs_data[job_id] = {
            'job_id': job_id,
            'status': tracker.status,
            'step': tracker.current_step,
            'total': tracker.total_steps,
            'logs': tracker.logs[-10:],  # Last 10 logs
            'cancellable': tracker.status == 'running'
        }
    
    return jsonify(jobs_data)


@app.route('/api/jobs/<job_id>/cancel', methods=['POST'])
def cancel_job(job_id):
    """Cancel a running job"""
    if job_id in job_cancellation:
        job_cancellation[job_id] = True
        return jsonify({'success': True, 'message': 'Job cancellation requested'})
    
    if job_id in active_jobs:
        # Job exists but cancellation flag doesn't (maybe completed)
        return jsonify({'success': False, 'message': 'Job cannot be cancelled'})
    
    return jsonify({'success': False, 'message': 'Job not found'}), 404


@app.route('/api/content/<project_name>/<filename>')
def get_content_file(project_name, filename):
    """Get content file"""
    content_dir = os.path.join('projects', project_name, '2_content')
    return send_from_directory(content_dir, filename)


@app.route('/api/export/<project_name>')
def export_project(project_name):
    """Export project as JSON"""
    project_dir = os.path.join('projects', project_name)
    data_file = os.path.join(project_dir, '3_company_data.json')
    
    if os.path.exists(data_file):
        return send_from_directory(
            os.path.dirname(data_file),
            os.path.basename(data_file),
            as_attachment=True,
            download_name=f"{project_name}_data.json"
        )
    
    return jsonify({'error': 'Data file not found'}), 404


@app.route('/api/download/<project_name>/urls')
def download_urls(project_name):
    """Download URLs CSV"""
    project_dir = os.path.join('projects', project_name)
    urls_file = os.path.join(project_dir, '1_urls.csv')
    
    if os.path.exists(urls_file):
        return send_from_directory(
            os.path.dirname(urls_file),
            os.path.basename(urls_file),
            as_attachment=True,
            download_name=f"{project_name}_urls.csv"
        )
    
    return jsonify({'error': 'URLs file not found'}), 404


@app.route('/api/download/<project_name>/content')
def download_content(project_name):
    """Download all content files as ZIP"""
    import zipfile
    from io import BytesIO
    
    project_dir = os.path.join('projects', project_name)
    content_dir = os.path.join(project_dir, '2_content')
    
    if not os.path.exists(content_dir):
        return jsonify({'error': 'Content directory not found'}), 404
    
    # Create ZIP in memory
    memory_file = BytesIO()
    with zipfile.ZipFile(memory_file, 'w', zipfile.ZIP_DEFLATED) as zf:
        for filename in os.listdir(content_dir):
            if filename.endswith('.md'):
                file_path = os.path.join(content_dir, filename)
                zf.write(file_path, filename)
    
    memory_file.seek(0)
    
    from flask import send_file
    return send_file(
        memory_file,
        mimetype='application/zip',
        as_attachment=True,
        download_name=f"{project_name}_content.zip"
    )


@app.route('/api/download/<project_name>/logs')
def download_logs(project_name):
    """Download all logs as ZIP"""
    import zipfile
    from io import BytesIO
    
    project_dir = os.path.join('projects', project_name)
    logs_dir = os.path.join(project_dir, 'logs')
    
    if not os.path.exists(logs_dir):
        return jsonify({'error': 'Logs directory not found'}), 404
    
    # Create ZIP in memory
    memory_file = BytesIO()
    with zipfile.ZipFile(memory_file, 'w', zipfile.ZIP_DEFLATED) as zf:
        for filename in os.listdir(logs_dir):
            file_path = os.path.join(logs_dir, filename)
            if os.path.isfile(file_path):
                zf.write(file_path, filename)
    
    memory_file.seek(0)
    
    from flask import send_file
    return send_file(
        memory_file,
        mimetype='application/zip',
        as_attachment=True,
        download_name=f"{project_name}_logs.zip"
    )


# WebSocket events
@socketio.on('connect')
def handle_connect():
    """Client connected"""
    emit('connected', {'message': 'Connected to Marketing Crawler'})


@socketio.on('disconnect')
def handle_disconnect():
    """Client disconnected"""
    pass


if __name__ == '__main__':
    # Use fixed port from config or default to 5000
    port = 5000
    
    print(f"""
╔══════════════════════════════════════════════════════════════╗
║        MARKETING CRAWLER - WEB DASHBOARD                     ║
╚══════════════════════════════════════════════════════════════╝

Starting web server...
Dashboard: http://localhost:{port}

🔧 If port {port} is already in use:
   On macOS: Disable AirPlay Receiver in System Settings
   Or change port in config.yaml

Press Ctrl+C to stop
    """)
    
    try:
        socketio.run(app, host='0.0.0.0', port=port, debug=False, allow_unsafe_werkzeug=True)
    except OSError as e:
        if 'Address already in use' in str(e):
            print(f"\n❌ Error: Port {port} is already in use!")
            print("\n💡 Solutions:")
            print("   1. On macOS: System Settings → General → AirDrop & Handoff → Turn off 'AirPlay Receiver'")
            print("   2. Kill the process using the port: lsof -ti:{port} | xargs kill -9")
            print("   3. Use a different port by editing config.yaml")
            exit(1)
        else:
            raise


"""
IMScraper Flask Application
Handles job submission and status checking for the asynchronous scraper
"""

from flask import Flask, request, jsonify, send_file, render_template, make_response
from flask_cors import CORS
import os
import json
import uuid
import logging
import time
from datetime import datetime

# Flask app setup
app = Flask(__name__)
CORS(app)

# Configure paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
JOBS_DIR = os.path.join(BASE_DIR, 'jobs')
RESULTS_DIR = os.path.join(BASE_DIR, 'results')
LOGS_DIR = os.path.join(BASE_DIR, 'logs')

# Ensure directories exist
for directory in [JOBS_DIR, RESULTS_DIR, LOGS_DIR]:
    os.makedirs(directory, exist_ok=True)

# Logging setup
logging.basicConfig(
    filename=os.path.join(LOGS_DIR, 'app.log'),
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/submit_job', methods=['POST'])
def submit_job():
    """Submit a new job to be processed by the background worker"""
    try:
        # Get form data
        urls = request.form.getlist('urls[]')
        use_majestic = request.form.get('use_majestic') == 'true'
        use_ahrefs = request.form.get('use_ahrefs') == 'true'
        use_dataforseo = request.form.get('use_dataforseo') == 'true'

        majestic_api_key = request.form.get('majestic_api_key') if use_majestic else None
        ahrefs_api_key = request.form.get('ahrefs_api_key') if use_ahrefs else None
        dataforseo_api_key = request.form.get('dataforseo_api_key') if use_dataforseo else None

        # Validate inputs
        if not urls:
            return jsonify({"error": "No URLs provided"}), 400

        if not use_majestic and not use_ahrefs and not use_dataforseo:
            return jsonify({"error": "At least one API must be selected"}), 400

        if use_majestic and not majestic_api_key:
            return jsonify({"error": "Majestic API key is required when Majestic API is selected"}), 400

        if use_ahrefs and not ahrefs_api_key:
            return jsonify({"error": "Ahrefs API key is required when Ahrefs API is selected"}), 400

        if use_dataforseo and not dataforseo_api_key:
            return jsonify({"error": "DataForSEO API key is required when DataForSEO API is selected"}), 400

        # Clean URLs
        cleaned_urls = [url.strip() for url in urls if url.strip()]
        
        # Generate a unique job ID
        job_id = str(uuid.uuid4())
        
        # Create job data
        job_data = {
            "urls": cleaned_urls,
            "use_majestic": use_majestic,
            "use_ahrefs": use_ahrefs,
            "use_dataforseo": use_dataforseo,
            "majestic_api_key": majestic_api_key,
            "ahrefs_api_key": ahrefs_api_key,
            "dataforseo_api_key": dataforseo_api_key,
            "created_at": datetime.now().isoformat()
        }
        
        # Save job data to file for the background worker to pick up
        job_file_path = os.path.join(JOBS_DIR, f"{job_id}.json")
        with open(job_file_path, 'w') as f:
            json.dump(job_data, f)
            
        logging.info(f"Created new job {job_id} with {len(cleaned_urls)} URLs")
        
        return jsonify({
            "status": "submitted",
            "job_id": job_id,
            "message": "Job submitted successfully",
            "total_urls": len(cleaned_urls)
        })
        
    except Exception as e:
        logging.error(f"Error submitting job: {str(e)}", exc_info=True)
        return jsonify({"error": f"Failed to submit job: {str(e)}"}), 500

@app.route('/job_status/<job_id>', methods=['GET'])
def job_status(job_id):
    """Check the status of a job"""
    try:
        # Sanitize job_id to prevent directory traversal
        job_id = os.path.basename(job_id)
        
        status_file = os.path.join(JOBS_DIR, f"{job_id}.status")
        result_file = os.path.join(RESULTS_DIR, f"{job_id}.ods")
        
        # If status file doesn't exist, check if job file exists
        if not os.path.exists(status_file):
            job_file = os.path.join(JOBS_DIR, f"{job_id}.json")
            if os.path.exists(job_file):
                return jsonify({
                    "status": "queued",
                    "message": "Job is queued for processing"
                })
            else:
                return jsonify({
                    "status": "not_found",
                    "message": "Job not found"
                }), 404
                
        # Read status from file
        with open(status_file, 'r') as f:
            status_data = json.load(f)
            
        # Add result download URL if job is completed
        if status_data.get('status') == 'completed' and os.path.exists(result_file):
            status_data['download_url'] = f"/download_result/{job_id}"
            
        return jsonify(status_data)
        
    except Exception as e:
        logging.error(f"Error checking job status: {str(e)}", exc_info=True)
        return jsonify({
            "status": "error",
            "message": f"Error checking job status: {str(e)}"
        }), 500

@app.route('/download_result/<job_id>', methods=['GET'])
def download_result(job_id):
    """Download the results file for a completed job"""
    try:
        # Sanitize job_id to prevent directory traversal
        job_id = os.path.basename(job_id)
        
        # Check if result file exists
        result_file = os.path.join(RESULTS_DIR, f"{job_id}.ods")
        if not os.path.exists(result_file):
            return jsonify({
                "error": "Result file not found"
            }), 404
            
        # Check if job is actually complete
        status_file = os.path.join(JOBS_DIR, f"{job_id}.status")
        if os.path.exists(status_file):
            with open(status_file, 'r') as f:
                status_data = json.load(f)
                if status_data.get('status') != 'completed':
                    return jsonify({
                        "error": "Job is not yet complete"
                    }), 400
        
        # Send the file
        response = make_response(send_file(result_file, as_attachment=True))
        response.headers["Content-Disposition"] = f"attachment; filename=imscraper_results_{job_id[:8]}.ods"
        response.headers["Content-Type"] = "application/vnd.oasis.opendocument.spreadsheet"
        return response
        
    except Exception as e:
        logging.error(f"Error downloading result: {str(e)}", exc_info=True)
        return jsonify({
            "error": f"Failed to download result: {str(e)}"
        }), 500

@app.route('/cancel_job/<job_id>', methods=['POST'])
def cancel_job(job_id):
    """Cancel a job that is queued or in progress"""
    try:
        # Sanitize job_id to prevent directory traversal
        job_id = os.path.basename(job_id)
        
        job_file = os.path.join(JOBS_DIR, f"{job_id}.json")
        status_file = os.path.join(JOBS_DIR, f"{job_id}.status")
        
        # Remove job file if it exists (queued job)
        if os.path.exists(job_file):
            os.remove(job_file)
            
        # Update status to cancelled if job is in progress
        if os.path.exists(status_file):
            with open(status_file, 'r') as f:
                status_data = json.load(f)
                
            if status_data.get('status') == 'processing':
                with open(status_file, 'w') as f:
                    status_data['status'] = 'cancelled'
                    json.dump(status_data, f)
        
        return jsonify({
            "status": "success",
            "message": "Job cancelled successfully"
        })
        
    except Exception as e:
        logging.error(f"Error cancelling job: {str(e)}", exc_info=True)
        return jsonify({
            "error": f"Failed to cancel job: {str(e)}"
        }), 500

@app.route('/cleanup_old_jobs', methods=['POST'])
def cleanup_old_jobs():
    """Admin endpoint to clean up old jobs and results"""
    try:
        # Optional basic auth check could be added here
        
        count = 0
        current_time = time.time()
        max_age_seconds = 7 * 24 * 60 * 60  # 7 days
        
        # Clean up old status files
        for filename in os.listdir(JOBS_DIR):
            file_path = os.path.join(JOBS_DIR, filename)
            if os.path.isfile(file_path):
                file_age = current_time - os.path.getmtime(file_path)
                if file_age > max_age_seconds:
                    os.remove(file_path)
                    count += 1
                    
        # Clean up old result files
        for filename in os.listdir(RESULTS_DIR):
            file_path = os.path.join(RESULTS_DIR, filename)
            if os.path.isfile(file_path):
                file_age = current_time - os.path.getmtime(file_path)
                if file_age > max_age_seconds:
                    os.remove(file_path)
                    count += 1
        
        return jsonify({
            "status": "success",
            "message": f"Cleaned up {count} old files"
        })
        
    except Exception as e:
        logging.error(f"Error cleaning up old jobs: {str(e)}", exc_info=True)
        return jsonify({
            "error": f"Failed to clean up old jobs: {str(e)}"
        }), 500

if __name__ == '__main__':
    app.run(debug=True)
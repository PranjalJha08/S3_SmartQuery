from flask import Flask, jsonify, request, send_file
import boto3
import os
from datetime import datetime
import tempfile
from dotenv import load_dotenv
load_dotenv()
from collections import defaultdict
from flask import render_template
import re
from datetime import timedelta, date

app = Flask(__name__)

# S3 configuration - expects AWS credentials to be set in environment or config
S3_BUCKET = os.environ.get('S3_BUCKET')
s3_client = boto3.client('s3')

@app.route('/')
def health_check():
    return jsonify({'status': 'ok'})

@app.route('/search', methods=['GET'])
def search_files():
    prefix = request.args.get('prefix', '')
    try:
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
        files = []
        for obj in response.get('Contents', []):
            files.append({
                'key': obj['Key'],
                'size': obj['Size'],
                'last_modified': obj['LastModified'].isoformat()
            })
        return jsonify({'files': files})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/file-details', methods=['GET'])
def file_details():
    filename = request.args.get('filename')
    if not filename:
        return jsonify({'error': 'filename query parameter is required'}), 400
    try:
        response = s3_client.head_object(Bucket=S3_BUCKET, Key=filename)
        details = {
            'key': filename,
            'size': response['ContentLength'],
            'last_modified': response['LastModified'].isoformat(),
            'content_type': response.get('ContentType'),
            'uploader': response['Metadata'].get('uploader', 'unknown')
        }
        return jsonify(details)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400
    uploader = request.form.get('uploader', 'unknown')
    try:
        s3_client.upload_fileobj(
            file,
            S3_BUCKET,
            file.filename,
            ExtraArgs={
                'Metadata': {'uploader': uploader}
            }
        )
        return jsonify({'message': 'File uploaded successfully', 'filename': file.filename})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/delete', methods=['POST'])
def delete_file():
    data = request.get_json()
    filename = data.get('filename')
    if not filename:
        return jsonify({'error': 'filename is required'}), 400
    try:
        s3_client.delete_object(Bucket=S3_BUCKET, Key=filename)
        return jsonify({'message': 'File deleted successfully', 'filename': filename})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/download', methods=['GET'])
def download_file():
    filename = request.args.get('filename')
    if not filename:
        return jsonify({'error': 'filename query parameter is required'}), 400
    try:
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            s3_client.download_fileobj(S3_BUCKET, filename, tmp)
            tmp.flush()
            return send_file(tmp.name, as_attachment=True, download_name=filename)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/count-by-type', methods=['GET'])
def count_by_type():
    try:
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET)
        counts = defaultdict(int)
        for obj in response.get('Contents', []):
            ext = obj['Key'].split('.')[-1] if '.' in obj['Key'] else 'no_ext'
            counts[ext] += 1
        return jsonify(dict(counts))
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/files-by-date', methods=['GET'])
def files_by_date():
    date_str = request.args.get('date')
    if not date_str:
        return jsonify({'error': 'date query parameter is required (YYYY-MM-DD)'}), 400
    try:
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET)
        files = []
        for obj in response.get('Contents', []):
            if obj['LastModified'].date().isoformat() == date_str:
                files.append(obj['Key'])
        return jsonify({'files': files, 'count': len(files)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/uploads-by-user', methods=['GET'])
def uploads_by_user():
    try:
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET)
        user_counts = defaultdict(int)
        for obj in response.get('Contents', []):
            head = s3_client.head_object(Bucket=S3_BUCKET, Key=obj['Key'])
            uploader = head['Metadata'].get('uploader', 'unknown')
            user_counts[uploader] += 1
        return jsonify(dict(user_counts))
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/storage-by-date', methods=['GET'])
def storage_by_date():
    date_str = request.args.get('date')
    if not date_str:
        return jsonify({'error': 'date query parameter is required (YYYY-MM-DD)'}), 400
    try:
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET)
        total_bytes = 0
        for obj in response.get('Contents', []):
            if obj['LastModified'].date().isoformat() == date_str:
                total_bytes += obj['Size']
        total_gb = total_bytes / (1024 ** 3)
        return jsonify({'total_gb': total_gb})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/total-storage', methods=['GET'])
def total_storage():
    try:
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET)
        total_bytes = sum(obj['Size'] for obj in response.get('Contents', []))
        total_gb = total_bytes / (1024 ** 3)
        return jsonify({'total_gb': total_gb})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/dashboard')
def dashboard():
    # Gather analytics for display
    try:
        # Count by type
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET)
        files = response.get('Contents', [])
        counts = defaultdict(int)
        user_counts = defaultdict(int)
        total_bytes = 0
        for obj in files:
            ext = obj['Key'].split('.')[-1] if '.' in obj['Key'] else 'no_ext'
            counts[ext] += 1
            total_bytes += obj['Size']
            head = s3_client.head_object(Bucket=S3_BUCKET, Key=obj['Key'])
            uploader = head['Metadata'].get('uploader', 'unknown')
            user_counts[uploader] += 1
        total_gb = total_bytes / (1024 ** 3)
        return render_template('dashboard.html',
            counts=dict(counts),
            user_counts=dict(user_counts),
            total_gb=total_gb,
            files=files
        )
    except Exception as e:
        return f"Error: {e}", 500

def parse_size(size_str):
    size_str = size_str.lower().replace(' ', '')
    if size_str.endswith('kb'):
        return int(float(size_str[:-2]) * 1024)
    if size_str.endswith('mb'):
        return int(float(size_str[:-2]) * 1024 * 1024)
    if size_str.endswith('gb'):
        return int(float(size_str[:-2]) * 1024 * 1024 * 1024)
    return int(size_str)

@app.route('/query', methods=['POST'])
def query_box():
    data = request.get_json()
    user_query = data.get('query', '').strip().lower()
    try:
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET)
        files = response.get('Contents', [])
        # Pre-fetch metadata for all files
        file_infos = []
        for obj in files:
            head = s3_client.head_object(Bucket=S3_BUCKET, Key=obj['Key'])
            uploader = head['Metadata'].get('uploader', 'unknown').lower()
            file_infos.append({
                'key': obj['Key'],
                'size': obj['Size'],
                'last_modified': obj['LastModified'],
                'uploader': uploader
            })
        today = date.today()
        # How many files were uploaded today?
        if re.search(r'files? (were )?uploaded today', user_query):
            count = sum(1 for f in file_infos if f['last_modified'].date() == today)
            return jsonify({'result': f'{count} files were uploaded today.'})
        # How many files were uploaded in the last X days?
        match = re.search(r'files? (were )?uploaded in the last (\d+) days?', user_query)
        if match:
            days = int(match.group(2))
            since = today - timedelta(days=days)
            count = sum(1 for f in file_infos if f['last_modified'].date() >= since)
            return jsonify({'result': f'{count} files were uploaded in the last {days} days.'})
        # How many files were uploaded this week?
        if re.search(r'files? (were )?uploaded this week', user_query):
            week_start = today - timedelta(days=today.weekday())
            count = sum(1 for f in file_infos if f['last_modified'].date() >= week_start)
            return jsonify({'result': f'{count} files were uploaded this week.'})
        # How many files were uploaded this month?
        if re.search(r'files? (were )?uploaded this month', user_query):
            count = sum(1 for f in file_infos if f['last_modified'].year == today.year and f['last_modified'].month == today.month)
            return jsonify({'result': f'{count} files were uploaded this month.'})
        # How many files are larger than X?
        match = re.search(r'files? (are )?larger than ([\d\.]+ ?[kmg]?b)', user_query)
        if match:
            size = parse_size(match.group(2))
            count = sum(1 for f in file_infos if f['size'] > size)
            return jsonify({'result': f'{count} files are larger than {match.group(2)}.'})
        # How many <ext> files were uploaded this month?
        match = re.search(r'how many (\w+) files? (were )?uploaded this month', user_query)
        if match:
            ext = match.group(1)
            count = sum(1 for f in file_infos if f['key'].endswith(f'.{ext}') and f['last_modified'].year == today.year and f['last_modified'].month == today.month)
            return jsonify({'result': f'{count} {ext} files were uploaded this month.'})
        # Top N files taking max storage
        match = re.search(r'top (\d+) files? taking max storage', user_query)
        if match:
            n = int(match.group(1))
            top_files = sorted(file_infos, key=lambda x: x['size'], reverse=True)[:n]
            result = [f"{f['key']} ({f['size']} bytes)" for f in top_files]
            return jsonify({'result': f'Top {n} files by size: ' + ', '.join(result)})
        # Top N <ext> files by size
        match = re.search(r'top (\d+) (\w+) files? by size', user_query)
        if match:
            n = int(match.group(1))
            ext = match.group(2)
            filtered = [f for f in file_infos if f['key'].endswith(f'.{ext}')]
            top_files = sorted(filtered, key=lambda x: x['size'], reverse=True)[:n]
            result = [f"{f['key']} ({f['size']} bytes)" for f in top_files]
            return jsonify({'result': f'Top {n} {ext} files by size: ' + ', '.join(result)})
        # How many files are uploaded by <user>?
        match = re.match(r'how many files are uploaded by (.+)', user_query)
        if match:
            user = match.group(1).strip()
            count = sum(1 for f in file_infos if f['uploader'] == user)
            return jsonify({'result': f'{count} files uploaded by {user}.'})
        # Top N files uploaded by <user> this week
        match = re.search(r'top (\d+) files uploaded by (\w+) this week', user_query)
        if match:
            n = int(match.group(1))
            user = match.group(2)
            week_start = today - timedelta(days=today.weekday())
            filtered = [f for f in file_infos if f['uploader'] == user and f['last_modified'].date() >= week_start]
            top_files = sorted(filtered, key=lambda x: x['size'], reverse=True)[:n]
            result = [f"{f['key']} ({f['size']} bytes)" for f in top_files]
            return jsonify({'result': f'Top {n} files uploaded by {user} this week: ' + ', '.join(result)})
        # Fallback
        return jsonify({'result': 'Sorry, I did not understand your query.'})
    except Exception as e:
        return jsonify({'result': f'Error: {str(e)}'}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)

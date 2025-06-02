from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
import sqlite3
import os
from .models import AccessLog
import json
from datetime import datetime, timedelta
from collections import defaultdict, Counter
from kafka import KafkaProducer

DATA_LAKE_PATH = '/Users/saad/Documents/data_integration/data_lake_project/data_lake'
DB_PATH = os.path.expanduser('~/Documents/data_integration/data_lake_project/data_warehouse.db')

def log_access(request):
    if request.user.is_authenticated:
        folder_path = request.GET.get('folder_path') or request.data.get('folder_path')
        AccessLog.objects.create(
            user=request.user,
            endpoint=request.path,
            request_body=request.body.decode('utf-8'),
            folder_path=folder_path
        )

def send_to_kafka(transaction, topic='transaction_log'):
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    producer.send(topic, value=transaction)
    producer.flush()
    producer.close()

@api_view(['POST'])
@permission_classes([IsAuthenticated])
def add_permission(request):
    log_access(request)
    user_id = request.data.get('user_id')
    folder_path = request.data.get('folder_path')
    access_level = request.data.get('access_level', 'read')
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT OR IGNORE INTO permissions (user_id, folder_path, access_level) VALUES (?, ?, ?)",
            (user_id, folder_path, access_level)
        )
        conn.commit()
    return Response({'status': 'permission added'})

@api_view(['DELETE'])
@permission_classes([IsAuthenticated])
def remove_permission(request):
    log_access(request)
    user_id = request.data.get('user_id')
    folder_path = request.data.get('folder_path')
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "DELETE FROM permissions WHERE user_id=? AND folder_path=?",
            (user_id, folder_path)
        )
        conn.commit()
    return Response({'status': 'permission removed'})

@api_view(['GET'])
@permission_classes([IsAuthenticated])
def get_data_lake(request):
    log_access(request)
    user_id = request.user.id
    folder_path = request.GET.get('folder_path')
    page = int(request.GET.get('page', 1))
    page_size = min(int(request.GET.get('page_size', 10)), 10)

    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT 1 FROM permissions WHERE user_id=? AND folder_path=?",
            (user_id, folder_path)
        )
        if not cursor.fetchone():
            return Response({'error': 'Access denied'}, status=403)

    abs_folder_path = os.path.join(DATA_LAKE_PATH, folder_path.lstrip('/'))
    data = []
    if os.path.isdir(abs_folder_path):
        for file in os.listdir(abs_folder_path):
            if file.endswith('.json'):
                with open(os.path.join(abs_folder_path, file), 'r') as f:
                    for line in f:
                        try:
                            data.append(json.loads(line))
                        except Exception:
                            continue
    
    
    payment_method = request.GET.get('payment_method')
    country = request.GET.get('country')
    product_category = request.GET.get('product_category')
    status = request.GET.get('status')
    amount_gt = request.GET.get('amount_gt')
    amount_lt = request.GET.get('amount_lt')
    amount_eq = request.GET.get('amount_eq')
    rating_gt = request.GET.get('customer_rating_gt')
    rating_lt = request.GET.get('customer_rating_lt')
    rating_eq = request.GET.get('customer_rating_eq')

    def match(item):
        if payment_method and item.get('payment_method') != payment_method:
            return False
        if product_category and item.get('product_category') != product_category:
            return False
        if status and item.get('status') != status:
            return False
        if country and item.get('location', {}).get('country') != country:
            return False
        if amount_gt and not (item.get('amount', 0) > float(amount_gt)):
            return False
        if amount_lt and not (item.get('amount', 0) < float(amount_lt)):
            return False
        if amount_eq and not (item.get('amount', 0) == float(amount_eq)):
            return False
        rating = item.get('customer_rating')
        if rating_gt and not (rating is not None and rating > float(rating_gt)):
            return False
        if rating_lt and not (rating is not None and rating < float(rating_lt)):
            return False
        if rating_eq and not (rating is not None and rating == float(rating_eq)):
            return False
        return True

    data = [item for item in data if match(item)]
    
    fields = request.GET.get('fields')
    if fields:
        field_list = [f.strip() for f in fields.split(',')]
        def project(item):
            return {k: item.get(k) for k in field_list if k in item}
        data = [project(item) for item in data]

    start = (page - 1) * page_size
    end = start + page_size
    paginated = data[start:end]

    return Response({
        'count': len(data),
        'page': page,
        'page_size': page_size,
        'results': paginated
    })

@api_view(['GET'])
@permission_classes([IsAuthenticated])
def money_spent_last_5min(request):
    folder_path = request.GET.get('folder_path')
    abs_folder_path = os.path.join(DATA_LAKE_PATH, folder_path.lstrip('/'))
    now = datetime.utcnow()
    five_min_ago = now - timedelta(minutes=5)
    total = 0.0
    if os.path.isdir(abs_folder_path):
        for file in os.listdir(abs_folder_path):
            if file.endswith('.json'):
                with open(os.path.join(abs_folder_path, file), 'r') as f:
                    for line in f:
                        try:
                            item = json.loads(line)
                            ts = item.get('timestamp')
                            if ts:
                                ts_dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                                if ts_dt > five_min_ago:
                                    total += float(item.get('amount', 0))
                        except Exception:
                            continue
    return Response({'money_spent_last_5min': total})


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def total_spent_per_user_and_type(request):
    folder_path = request.GET.get('folder_path')
    abs_folder_path = os.path.join(DATA_LAKE_PATH, folder_path.lstrip('/'))
    stats = defaultdict(lambda: defaultdict(float))
    if os.path.isdir(abs_folder_path):
        for file in os.listdir(abs_folder_path):
            if file.endswith('.json'):
                with open(os.path.join(abs_folder_path, file), 'r') as f:
                    for line in f:
                        try:
                            item = json.loads(line)
                            user = item.get('user_id')
                            ttype = item.get('transaction_type')
                            amount = float(item.get('amount', 0))
                            if user and ttype:
                                stats[user][ttype] += amount
                        except Exception:
                            continue
    stats = {user: dict(types) for user, types in stats.items()}
    return Response(stats)

@api_view(['GET'])
@permission_classes([IsAuthenticated])
def top_x_products(request):
    folder_path = request.GET.get('folder_path')
    x = int(request.GET.get('x', 5))
    abs_folder_path = os.path.join(DATA_LAKE_PATH, folder_path.lstrip('/'))
    counter = Counter()
    if os.path.isdir(abs_folder_path):
        for file in os.listdir(abs_folder_path):
            if file.endswith('.json'):
                with open(os.path.join(abs_folder_path, file), 'r') as f:
                    for line in f:
                        try:
                            item = json.loads(line)
                            prod = item.get('product_id')
                            qty = int(item.get('quantity', 1))
                            if prod:
                                counter[prod] += qty
                        except Exception:
                            continue
    top = counter.most_common(x)
    return Response({'top_products': top})

@api_view(['GET'])
@permission_classes([IsAuthenticated])
def get_data_version(request):
    folder_path = request.GET.get('folder_path')
    version = request.GET.get('version')
    abs_folder_path = os.path.join(DATA_LAKE_PATH, folder_path.lstrip('/'))
    if not os.path.isdir(abs_folder_path):
        return Response({'error': 'Folder not found'}, status=404)
    file_name = f"transaction_log_v{version}.json" if version else "transaction_log.json"
    file_path = os.path.join(abs_folder_path, file_name)
    if not os.path.isfile(file_path):
        return Response({'error': 'Version not found'}, status=404)
    data = []
    with open(file_path, 'r') as f:
        for line in f:
            try:
                data.append(json.loads(line))
            except Exception:
                continue
    return Response({'count': len(data), 'results': data[:10]})


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def access_logs(request):
    folder_path = request.GET.get('folder_path')
    logs = AccessLog.objects.all()
    if folder_path:
        logs = logs.filter(folder_path=folder_path)
    logs = logs.order_by('-datetime')[:50]
    results = []
    for log in logs:
        results.append({
            'user': log.user.username,
            'endpoint': log.endpoint,
            'timestamp': log.datetime,
            'request_body': log.request_body,
            'folder_path': log.folder_path,
        })
    return Response({'logs': results})

@api_view(['GET'])
@permission_classes([IsAuthenticated])
def list_resources(request):
    resources = []
    for root, dirs, files in os.walk(DATA_LAKE_PATH):
        for file in files:
            if file.endswith('.json'):
                rel_path = os.path.relpath(os.path.join(root, file), DATA_LAKE_PATH)
                resources.append(rel_path)
    return Response({'resources': resources})

@api_view(['GET'])
@permission_classes([IsAuthenticated])
def full_text_search(request):
    query = request.GET.get('query')
    date_from = request.GET.get('date_from')
    if not query:
        return Response({'error': 'query parameter is required'}, status=400)
    found = []
    files_found = set()
    for root, dirs, files in os.walk(DATA_LAKE_PATH):
        for file in files:
            if file.endswith('.json'):
                file_path = os.path.join(root, file)
                with open(file_path, 'r') as f:
                    for line in f:
                        try:
                            obj = json.loads(line)
                            if date_from:
                                ts = obj.get('timestamp')
                                if ts and ts < date_from:
                                    continue
                            if query in json.dumps(obj):
                                found.append(obj)
                                files_found.add(os.path.relpath(file_path, DATA_LAKE_PATH))
                        except Exception:
                            continue
    return Response({
        'count': len(found),
        'results': found[:20],
        'files': list(files_found)
    })

@api_view(['POST'])
@permission_classes([IsAuthenticated])
def train_ml_model(request):

    import time
    time.sleep(2) 
    return Response({'status': 'training started', 'model': 'dummy_model'})


@api_view(['POST'])
@permission_classes([IsAuthenticated])
def repush_transaction(request):
    transaction_id = request.data.get('transaction_id')
    folder_path = request.data.get('folder_path')
    if not transaction_id or not folder_path:
        return Response({'error': 'transaction_id and folder_path required'}, status=400)
    abs_folder_path = os.path.join(DATA_LAKE_PATH, folder_path.lstrip('/'))
    found = None
    if os.path.isdir(abs_folder_path):
        for file in os.listdir(abs_folder_path):
            if file.endswith('.json'):
                with open(os.path.join(abs_folder_path, file), 'r') as f:
                    for line in f:
                        try:
                            obj = json.loads(line)
                            if obj.get('transaction_id') == transaction_id:
                                found = obj
                                break
                        except Exception:
                            continue
    if not found:
        return Response({'error': 'Transaction not found'}, status=404)
    found['timestamp'] = datetime.utcnow().isoformat() + 'Z'
    send_to_kafka(found)
    return Response({'status': 'repushed', 'transaction': found})

@api_view(['POST'])
@permission_classes([IsAuthenticated])
def repush_all_transactions(request):
    folder_path = request.data.get('folder_path')
    if not folder_path:
        return Response({'error': 'folder_path required'}, status=400)
    abs_folder_path = os.path.join(DATA_LAKE_PATH, folder_path.lstrip('/'))
    count = 0
    if os.path.isdir(abs_folder_path):
        for file in os.listdir(abs_folder_path):
            if file.endswith('.json'):
                with open(os.path.join(abs_folder_path, file), 'r') as f:
                    for line in f:
                        try:
                            obj = json.loads(line)
                            obj['timestamp'] = datetime.utcnow().isoformat() + 'Z'
                            send_to_kafka(obj)
                            count += 1
                        except Exception:
                            continue
    return Response({'status': 'repushed_all', 'count': count})
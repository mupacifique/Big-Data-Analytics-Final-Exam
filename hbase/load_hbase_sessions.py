import json
import happybase
import os
import time

def connect_hbase(max_retries=5):
    """Connect to HBase with retry logic"""
    for attempt in range(max_retries):
        try:
            connection = happybase.Connection('localhost', port=9090, timeout=60000)
            connection.open()
            print("✔ Connected to HBase")
            return connection
        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                print(f"Connection attempt {attempt + 1} failed: {str(e)[:100]}")
                print(f"Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                raise

connection = connect_hbase()

try:
    table = connection.table('sessions')
    print("✔ Connected to 'sessions' table")
except Exception as e:
    print(f"✗ Error connecting to table: {e}")
    connection.close()
    exit(1)

# Load all sessions files from sessions_0.json to sessions_19.json
data_dir = os.path.join(os.path.dirname(__file__), '..', 'data', 'raw')
total_sessions = 0
batch_size = 100  # Smaller batch size for better stability

for file_num in range(20):
    data_path = os.path.join(data_dir, f'sessions_{file_num}.json')
    
    if not os.path.exists(data_path):
        print(f"⚠ File not found: {data_path}")
        continue
    
    print(f"\nLoading sessions_{file_num}.json...")
    
    try:
        with open(data_path, 'r', encoding='utf-8') as f:
            sessions = json.load(f)
        
        # Process in smaller batches
        for batch_start in range(0, len(sessions), batch_size):
            batch_end = min(batch_start + batch_size, len(sessions))
            batch = sessions[batch_start:batch_end]
            
            retry_count = 0
            max_retries = 3
            
            while retry_count < max_retries:
                try:
                    for s in batch:
                        # Row key: session_id
                        row_key = s['session_id'].encode()

                        row = {
                            b'meta:user_id': str(s.get('user_id', '')).encode(),
                            b'meta:timestamp': str(s.get('timestamp', '')).encode(),

                            b'geo:country': s.get('geo', {}).get('country', '').encode(),
                            b'geo:city': s.get('geo', {}).get('city', '').encode(),

                            b'device:type': s.get('device', {}).get('type', '').encode(),
                            b'device:os': s.get('device', {}).get('os', '').encode(),

                            b'stats:duration': str(s.get('stats', {}).get('duration', 0)).encode(),
                            b'stats:pages': str(s.get('stats', {}).get('pages', 0)).encode(),
                        }

                        table.put(row_key, row)
                    
                    break  # Batch successful
                    
                except Exception as e:
                    retry_count += 1
                    if retry_count < max_retries:
                        wait_time = 2 ** retry_count
                        print(f"  Batch write error, retry {retry_count}/{max_retries} in {wait_time}s...")
                        time.sleep(wait_time)
                    else:
                        print(f"  ✗ Failed to write batch after {max_retries} retries: {str(e)[:100]}")
                        raise
            
            total_sessions += len(batch)
            if (batch_start // batch_size) % 100 == 0:
                print(f"  Progress: {batch_start + len(batch):,} sessions processed")
        
        print(f"✔ Loaded {len(sessions):,} sessions from sessions_{file_num}.json")
        
    except Exception as e:
        print(f"✗ Error loading sessions_{file_num}.json: {str(e)[:200]}")
        continue

connection.close()
print(f"\n✔ Total: {total_sessions:,} sessions loaded into HBase")
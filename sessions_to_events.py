"""
Convert complex session data to simple event format for Spark analytics
Reads sessions_*.json and outputs simple event JSON files
"""

import json
import os
from pathlib import Path

# Paths
INPUT_DIR = Path("data/raw")
OUTPUT_DIR = Path("data/raw_events")

# Mapping from page_type to standard funnel events
PAGE_TO_EVENT = {
    "product_detail": "view_item",
    "cart": "add_to_cart",
    "checkout": "begin_checkout",
    "confirmation": "purchase"
}

def convert_sessions_to_events():
    """Convert session files to simple event format"""
    
    # Create output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    print("🔄 Converting session data to event format...")
    print(f"   Input: {INPUT_DIR}")
    print(f"   Output: {OUTPUT_DIR}")
    print()
    
    all_events = []
    total_sessions = 0
    
    # Find all session files
    session_files = sorted(INPUT_DIR.glob("sessions_*.json"))
    
    if not session_files:
        print("❌ ERROR: No session files found!")
        print(f"   Looking in: {INPUT_DIR.absolute()}")
        print("   Expected files: sessions_0.json, sessions_1.json, etc.")
        return
    
    print(f"Found {len(session_files)} session file(s)")
    
    # Process each session file
    for session_file in session_files:
        print(f"📂 Processing {session_file.name}...")
        
        try:
            with open(session_file, 'r') as f:
                sessions = json.load(f)
            
            total_sessions += len(sessions)
            
            # Extract events from each session
            for session in sessions:
                user_id = session['user_id']
                page_views = session.get('page_views', [])
                
                # Convert each page view to an event
                for page_view in page_views:
                    page_type = page_view.get('page_type')
                    timestamp = page_view.get('timestamp')
                    
                    # Only include pages that map to funnel events
                    if page_type in PAGE_TO_EVENT:
                        event = {
                            "user_id": user_id,
                            "event": PAGE_TO_EVENT[page_type],
                            "timestamp": timestamp
                        }
                        all_events.append(event)
            
            print(f"   ✅ Processed {len(sessions):,} sessions")
            
        except json.JSONDecodeError as e:
            print(f"   ❌ ERROR: Invalid JSON in {session_file.name}: {e}")
        except Exception as e:
            print(f"   ❌ ERROR processing {session_file.name}: {e}")
    
    print(f"\n📊 Conversion Summary:")
    print(f"   Total sessions processed: {total_sessions:,}")
    print(f"   Total events extracted: {len(all_events):,}")
    
    if len(all_events) == 0:
        print("\n❌ ERROR: No events were extracted!")
        print("   Possible issues:")
        print("   1. Sessions have no page_views")
        print("   2. Page types don't match expected funnel events")
        print("   3. Session JSON structure is different than expected")
        return
    
    # Count events by type
    event_counts = {}
    for event in all_events:
        event_type = event['event']
        event_counts[event_type] = event_counts.get(event_type, 0) + 1
    
    print("\n   Event breakdown:")
    for event_type in ['view_item', 'add_to_cart', 'begin_checkout', 'purchase']:
        count = event_counts.get(event_type, 0)
        percentage = (count / len(all_events)) * 100 if all_events else 0
        print(f"     • {event_type:20s}: {count:7,} ({percentage:5.2f}%)")
    
    # Write events to output files (split into chunks)
    EVENTS_PER_FILE = 250000
    num_files = (len(all_events) + EVENTS_PER_FILE - 1) // EVENTS_PER_FILE
    
    print(f"\n💾 Writing {num_files} event file(s)...")
    
    for i in range(num_files):
        start_idx = i * EVENTS_PER_FILE
        end_idx = min(start_idx + EVENTS_PER_FILE, len(all_events))
        chunk = all_events[start_idx:end_idx]
        
        output_file = OUTPUT_DIR / f"events_{i+1:03d}.json"
        
        # Write one JSON object per line (newline-delimited JSON)
        with open(output_file, 'w') as f:
            for event in chunk:
                json.dump(event, f)
                f.write('\n')
        
        print(f"   ✅ {output_file.name}: {len(chunk):,} events")
    
    # Verify one file
    print("\n🔍 Verifying output format...")
    sample_file = OUTPUT_DIR / "events_001.json"
    with open(sample_file, 'r') as f:
        lines = [f.readline() for _ in range(3)]
    
    print("   First 3 lines:")
    for i, line in enumerate(lines, 1):
        if line.strip():
            event = json.loads(line)
            print(f"     {i}. {event}")
    
    print("\n" + "="*70)
    print("✅ CONVERSION COMPLETE!")
    print("="*70)
    print(f"\n📁 Event files saved in: {OUTPUT_DIR.absolute()}")
    print(f"📊 Total events: {len(all_events):,}")
    print(f"📊 Total users: {len(set(e['user_id'] for e in all_events)):,}")
    print(f"📊 Avg events per user: {len(all_events) / len(set(e['user_id'] for e in all_events)):.2f}")
    
    print("\n🎯 Next steps:")
    print("   1. Update funnel_analysis.py to read from 'data/raw_events/*.json'")
    print("   2. Or copy event files to 'data/raw/' and delete old session files")
    print("   3. Run: python analytics/funnel_analysis.py")
    print("="*70)

if __name__ == "__main__":
    convert_sessions_to_events()

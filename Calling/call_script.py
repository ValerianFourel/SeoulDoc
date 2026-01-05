import os
import time
import requests
from twilio.rest import Client
from dotenv import load_dotenv
from pathlib import Path

# --- CONFIGURATION ---

env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

account_sid = os.getenv('TWILIO_ACCOUNT_SID')
auth_token = os.getenv('TWILIO_AUTH_TOKEN')

if not account_sid or not auth_token:
    print(f"Error: Could not find credentials in {env_path}")
    exit()

my_twilio_number = '+14632787395'
person_to_call = '+821062712111' 

# Your Main Call Script URL (The one with timeout="1")
script_url = 'https://handler.twilio.com/twiml/EH65ca400b5de1e336b3cc070590d63944'

base_folder = os.path.dirname(os.path.abspath(__file__))
data_folder = os.path.join(base_folder, "data")

if not os.path.exists(data_folder):
    os.makedirs(data_folder)

client = Client(account_sid, auth_token)

# --- EXECUTION ---

print(f"Dialing {person_to_call}...")

try:
    # NOTE: record=True enables "Full Call Recording"
    call = client.calls.create(
        to=person_to_call,
        from_=my_twilio_number,
        url=script_url,
        record=True 
    )
    print(f"Call initiated! SID: {call.sid}")
except Exception as e:
    print(f"Failed to initiate call: {e}")
    exit()

# 1. Wait for Call to End
print("Waiting for call to complete...")
while True:
    c = client.calls(call.sid).fetch()
    if c.status in ['completed', 'failed', 'busy', 'no-answer', 'canceled']:
        print(f"Call ended with status: {c.status}")
        break
    time.sleep(2)

if c.status != 'completed':
    print("Call not completed successfully.")
    exit()

# 2. Find ALL Recordings (Full Call + Answer Segment)
print("Searching for recordings (Full Call + Answer Segment)...")
# Give Twilio a moment to process the files
time.sleep(5) 

recordings = client.recordings.list(call_sid=call.sid)
retry = 0
while not recordings and retry < 5:
    print("Waiting for recordings to appear...")
    time.sleep(3)
    recordings = client.recordings.list(call_sid=call.sid)
    retry += 1

print(f"Found {len(recordings)} recordings.")

# 3. Loop through EVERY recording found
for rec in recordings:
    rec_sid = rec.sid
    duration = rec.duration or "0"
    print(f"\n--- Processing Recording {rec_sid} (Duration: {duration}s) ---")
    
    # A. Download Audio
    mp3_filename = f"audio_{rec_sid}_dur{duration}.mp3"
    mp3_path = os.path.join(data_folder, mp3_filename)
    mp3_url = f"https://api.twilio.com/2010-04-01/Accounts/{account_sid}/Recordings/{rec_sid}.mp3"

    try:
        response = requests.get(mp3_url, auth=(account_sid, auth_token))
        if response.status_code == 200:
            with open(mp3_path, 'wb') as f:
                f.write(response.content)
            print(f"   Saved Audio: {mp3_filename}")
        else:
            print(f"   Failed download: {response.status_code}")
    except Exception as e:
        print(f"   Download error: {e}")

    # B. Check for Transcript (Only the 'Answer' recording will have this)
    # We use the specific recording object to find its transcripts
    print("   Checking for transcript...")
    transcription_text = None
    
    # Check if transcripts exist for THIS specific recording
    # We do a quick loop to wait for it if it's processing
    for i in range(5):
        t_list = client.recordings(rec_sid).transcriptions.list()
        if t_list:
            transcription_text = t_list[0].transcription_text
            break
        time.sleep(1)

    if transcription_text:
        txt_filename = f"transcript_{rec_sid}.txt"
        txt_path = os.path.join(data_folder, txt_filename)
        with open(txt_path, 'w') as f:
            f.write(f"Source Recording: {rec_sid}\n")
            f.write("-" * 20 + "\n")
            f.write(transcription_text)
        print(f"   ✅ FOUND TRANSCRIPT! Saved to {txt_filename}")
    else:
        print("   (No transcript for this file - likely the full call recording)")

print("\nDone! Check your /data folder.")
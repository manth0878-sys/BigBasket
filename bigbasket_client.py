#!/usr/bin/env python3
"""
BigBasket API Client
"""

import re
import time
import json
import requests
from typing import Optional, Dict, Any


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
import os
from flask import Flask, jsonify, request
from dotenv import load_dotenv
import requests
import firebase_admin
from datetime import datetime, timedelta
from firebase_admin import credentials, messaging

# .env 파일에서 환경 변수를 로드합니다.
# 이 파일의 위치에 따라 .env 파일 경로를 조정해야 할 수 있습니다.
# 예: load_dotenv(dotenv_path='../.env')
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

# Firebase Admin SDK 초기화
# GOOGLE_APPLICATION_CREDENTIALS 환경 변수를 사용하여 자동으로 인증 정보를 찾습니다.
try:
    if not firebase_admin._apps:
        cred = credentials.ApplicationDefault()
        firebase_admin.initialize_app(cred, {
            'projectId': 'locationtracker-jennycoffee',
        })
except Exception as e:
    print(f"Firebase Admin SDK 초기화 실패: {e}")

# Helper function to execute D1 queries and return raw Cloudflare API response
def _call_d1_api(sql_query):
    cf_api_token = os.getenv('CF_API_TOKEN')
    cf_account_id = os.getenv('CF_ACCOUNT_ID')
    cf_d1_database_id = os.getenv('CF_D1_DATABASE_ID')

    if not all([cf_api_token, cf_account_id, cf_d1_database_id]):
        raise ValueError('Server configuration error: API token, Account ID, or D1 Database ID is missing.')

    api_url = f"https://api.cloudflare.com/client/v4/accounts/{cf_account_id}/d1/database/{cf_d1_database_id}/query"

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {cf_api_token}",
    }
    
    data = {"sql": sql_query}
    response = requests.post(api_url, headers=headers, json=data)
    response.raise_for_status() # 2xx 상태 코드가 아니면 예외를 발생시킵니다.
    return response.json() # Cloudflare D1 API의 전체 JSON 응답을 반환합니다.

app = Flask(__name__)

# Cloudflare D1 데이터베이스에 직접 쿼리를 실행하는 API 엔드포인트
@app.route('/api/d1/execute-query', methods=['GET']) # GET으로 변경
def execute_d1_query():
    # 환경 변수에서 Cloudflare 설정값을 가져옵니다.
    cf_api_token = os.getenv('CF_API_TOKEN')
    cf_account_id = os.getenv('CF_ACCOUNT_ID')
    cf_d1_database_id = os.getenv('CF_D1_DATABASE_ID')

    if not all([cf_api_token, cf_account_id, cf_d1_database_id]):
        return jsonify({'error': 'Server configuration error: API token, Account ID, or D1 Database ID is missing.'}), 500

    sql_query = request.args.get('sql')
    if not sql_query:
        return jsonify({'error': 'SQL query is missing.'}), 400

    try:
        cf_response = _call_d1_api(sql_query)
        return jsonify(cf_response)
    except requests.exceptions.RequestException as e:
        return jsonify({'error': str(e), 'details': e.response.text if e.response else 'No response'}), 500
    except ValueError as e: # _call_d1_api에서 환경 변수 누락 시 발생
        return jsonify({'error': str(e)}), 500

# FCM 알림을 보내는 API 엔드포인트
@app.route('/api/fcm/send', methods=['GET'])
def send_fcm_notification():
    # URL 쿼리 파라미터에서 데이터를 가져옵니다.
    token = request.args.get('token') # 클라이언트(앱)의 FCM 등록 토큰
    title = request.args.get('title')
    body = request.args.get('body')

    if not all([token, title, body]):
        return jsonify({'error': 'token, title, body 필드는 필수입니다.'}), 400

    # FCM 메시지 구성
    message = messaging.Message(
        notification=messaging.Notification(
            title=title,
            body=body,
        ),
        token=token,
    )

    try:
        # 메시지 전송
        response = messaging.send(message)
        # 성공 시 메시지 ID 반환
        print('Successfully sent message:', response)
        return jsonify({'success': True, 'messageId': response})
    except Exception as e:
        print('Error sending message:', e)
        # Firebase 에러 객체를 문자열로 변환하여 더 자세한 정보 제공
        return jsonify({'success': False, 'error': str(e)}), 500

# 새로운 API 엔드포인트: 마지막 기록이 1시간 이상인 기기에 알림 전송
@app.route('/api/check-and-notify-inactive', methods=['GET']) # POST로 유지하여 작업 실행을 명시
def check_and_notify_inactive_devices():
    notification_results = []
    try:
        # 1. DeviceRelationNoti 테이블에서 모든 관계(DeviceId, toDeviceId) 목록을 가져옵니다.
        # 가정: DeviceRelationNoti 테이블은 DeviceId (TEXT)와 toDeviceId (TEXT) 컬럼을 가집니다.
        relation_query = "SELECT DeviceId, toDeviceId FROM DeviceRelationNoti"
        relation_cf_response = _call_d1_api(relation_query)
        
        device_relations = []
        # Cloudflare D1 Query API의 응답 구조는 `{"results": [{"results": [...]}]}` 형태입니다.
         
        if relation_cf_response.get('result') and relation_cf_response['result'] and relation_cf_response['result'][0].get('results'):
            device_relations = relation_cf_response['result'][0]['results']
        if not device_relations:
            return jsonify({'message': 'No device relations found. No notifications sent.'}), 200

        for relation in device_relations:
            from_device_id = relation.get('DeviceId')
            target_device_id = relation.get('toDeviceId')

            if not all([from_device_id, target_device_id]):
                continue

            # 2. Locations 테이블에서 from_device_id의 마지막 created_at을 가져옵니다.
            # 가정: Locations 테이블은 DeviceId (TEXT)와 created_at (TEXT) 컬럼을 가집니다.
            location_query = f"SELECT created_at FROM Locations WHERE DeviceId = '{from_device_id}' ORDER BY created_at DESC LIMIT 1"
            location_cf_response = _call_d1_api(location_query)

            last_created_at_str = None
            if location_cf_response.get('result') and location_cf_response['result'] and location_cf_response['result'][0].get('results'):
                last_created_at_str = location_cf_response['result'][0]['results'][0]['created_at']

            if not last_created_at_str:
                notification_results.append({
                    'toDeviceId': target_device_id,
                    'status': 'skipped',
                    'reason': f'No location data found for from_device_id: {from_device_id}.'
                })
                continue

            # created_at 시간을 파싱하고 1시간 이상 경과했는지 확인합니다.
            try:
                # ISO 8601 형식 (예: '2025-09-26T14:13:00.158Z') 파싱 시도
                last_created_at = datetime.strptime(last_created_at_str, '%Y-%m-%dT%H:%M:%S.%fZ')
            except ValueError:
                # 일반적인 'YYYY-MM-DD HH:MM:SS' 형식 파싱 (fallback)
                last_created_at = datetime.strptime(last_created_at_str, '%Y-%m-%d %H:%M:%S')

            one_hour_ago = datetime.utcnow() - timedelta(hours=1)

            if last_created_at < one_hour_ago:
                # 3. Devices 테이블에서 toDeviceId의 notiToken을 가져옵니다.
                # 가정: Devices 테이블은 id (TEXT)와 notiToken (TEXT) 컬럼을 가집니다.
                device_query = f"SELECT notiToken FROM Devices WHERE id = '{target_device_id}' and setAllowNoti = 1"
                device_cf_response = _call_d1_api(device_query)

                noti_token = None
                if device_cf_response.get('result') and device_cf_response['result'] and device_cf_response['result'][0].get('results'):
                    noti_token = device_cf_response['result'][0]['results'][0].get('notiToken')

                if not noti_token:
                    notification_results.append({
                        'toDeviceId': target_device_id,
                        'status': 'skipped',
                        'reason': 'No notification token found for this device.'
                    })
                    continue

                # 4. FCM 알림을 전송합니다.
                try:
                    message = messaging.Message(
                        notification=messaging.Notification(
                            title="활동 알림",
                            body=f"연결된 기기({from_device_id})가 1시간 이상 활동이 없습니다."
                        ),
                        token=noti_token,
                    )
                    fcm_response = messaging.send(message)
                    notification_results.append({'toDeviceId': target_device_id, 'status': 'sent', 'messageId': fcm_response})
                except Exception as fcm_e:
                    notification_results.append({'toDeviceId': target_device_id, 'status': 'failed_fcm', 'error': str(fcm_e)})
            else:
                notification_results.append({'toDeviceId': target_device_id, 'status': 'skipped', 'reason': 'Last activity within 1 hour.'})

    except (requests.exceptions.RequestException, ValueError) as e:
        print(f"Error in check_and_notify_inactive_devices: {e}")
        return jsonify({'error': str(e)}), 500
    except Exception as e:
        print(f"Unexpected error in check_and_notify_inactive_devices: {e}")
        return jsonify({'error': f"An unexpected error occurred: {str(e)}"}), 500

    return jsonify({'status': 'completed', 'results': notification_results}), 200


if __name__ == '__main__':
    app.run(port=3001, debug=True) # 기존 앱(3000번)과 충돌을 피하기 위해 포트를 3001로 설정했습니다.
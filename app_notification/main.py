import os
from flask import Flask, jsonify, request
from dotenv import load_dotenv
import requests
import firebase_admin
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

    # 클라이언트로부터 URL 쿼리 파라미터로 SQL 쿼리를 받습니다.
    sql_query = request.args.get('sql')
    if not sql_query:
        return jsonify({'error': 'SQL query is missing.'}), 400

    api_url = f"https://api.cloudflare.com/client/v4/accounts/{cf_account_id}/d1/database/{cf_d1_database_id}/query"

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {cf_api_token}",
    }
    
    data = {"sql": sql_query}

    try:
        response = requests.post(api_url, headers=headers, json=data)
        response.raise_for_status()
        return jsonify(response.json())
    except requests.exceptions.RequestException as e:
        return jsonify({'error': str(e), 'details': response.text if 'response' in locals() else 'No response'}), 500

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


if __name__ == '__main__':
    app.run(port=3001, debug=True) # 기존 앱(3000번)과 충돌을 피하기 위해 포트를 3001로 설정했습니다.
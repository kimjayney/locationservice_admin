import os
from datetime import datetime, timedelta
from flask import Flask, jsonify, request, send_from_directory
from dotenv import load_dotenv
import requests

# .env 파일에서 환경 변수를 로드합니다.
load_dotenv()

# Flask 앱을 생성합니다.
# static_folder를 프로젝트 루트 디렉토리('..')로 설정하여 index.html을 찾을 수 있게 합니다.
app = Flask(__name__, static_folder=os.path.join(os.path.dirname(__file__), '..'), static_url_path='')

# 루트 경로 요청 시 index.html 파일을 서빙합니다.
@app.route('/')
def index():
    return send_from_directory(app.static_folder, 'index.html')

# D1 Insights 데이터를 가져오는 API 엔드포인트
@app.route('/api/insights', methods=['GET'])
def get_d1_insights():
    cf_api_token = os.getenv('CF_API_TOKEN')
    cf_account_id = os.getenv('CF_ACCOUNT_ID')
    cf_d1_database_id = os.getenv('CF_D1_DATABASE_ID') # .env에서 데이터베이스 ID 로드

    if not all([cf_api_token, cf_account_id, cf_d1_database_id]):
        return jsonify({'error': 'Server configuration error: API token, Account ID, or D1 Database ID is missing.'}), 500

    api_url = "https://api.cloudflare.com/client/v4/graphql"

    # 프론트엔드에서 받은 날짜 파라미터를 사용하고, 없으면 기본값(최근 1일)을 사용합니다.
    default_until_date = datetime.utcnow().strftime('%Y-%m-%d')
    default_since_date = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')

    since_date = request.args.get('start_date', default_since_date)
    until_date = request.args.get('end_date', default_until_date)


    # 제공된 쿼리와 동일하게 구조를 수정합니다.
    query = f"""
        query {{
            viewer {{
                accounts(filter: {{ accountTag: "{cf_account_id}" }}) {{
                    d1AnalyticsAdaptiveGroups(
                        limit: 10000
                        filter: {{
                            date_geq: "{since_date}",
                            date_leq: "{until_date}",
                            databaseId: "{cf_d1_database_id}"
                        }}
                        orderBy: [date_DESC]
                    ) {{
                        dimensions {{
                            date
                            databaseId
                        }}
                        sum {{
                            readQueries
                            writeQueries
                        }}
                    }}
                }}
            }}
        }}
    """

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {cf_api_token}",
    }

    try:
        response = requests.post(api_url, headers=headers, json={'query': query})
        response.raise_for_status()  # 2xx 상태 코드가 아니면 예외를 발생시킵니다.
        return jsonify(response.json())
    except requests.exceptions.RequestException as e:
        print(f"Error fetching D1 insights: {e}")
        return jsonify({'error': str(e)}), 500

# 최근 실행된 쿼리 목록을 가져오는 API 엔드포인트
@app.route('/api/queries', methods=['GET'])
def get_d1_queries():
    cf_api_token = os.getenv('CF_API_TOKEN')
    cf_account_id = os.getenv('CF_ACCOUNT_ID')
    cf_d1_database_id = os.getenv('CF_D1_DATABASE_ID')

    if not all([cf_api_token, cf_account_id, cf_d1_database_id]):
        return jsonify({'error': 'Server configuration error: API token, Account ID, or D1 Database ID is missing.'}), 500

    api_url = "https://api.cloudflare.com/client/v4/graphql"

    # 프론트엔드에서 받은 날짜 파라미터를 사용하고, 없으면 기본값(최근 1일)을 사용합니다.
    default_until_date = datetime.utcnow().strftime('%Y-%m-%d')
    default_since_date = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')

    since_date = request.args.get('start_date', default_since_date)
    until_date = request.args.get('end_date', default_until_date)

    # d1QueriesAdaptiveGroups를 사용하여 개별 쿼리 이벤트를 가져옵니다.
    query = f"""
        query {{
            viewer {{
                accounts(filter: {{ accountTag: "{cf_account_id}" }}) {{
                    d1QueriesAdaptiveGroups(
                        limit: 100,
                        filter: {{
                            date_geq: "{since_date}",
                            date_leq: "{until_date}",
                            databaseId: "{cf_d1_database_id}"
                        }},
                        orderBy: [date_DESC]
                    ) {{
                        dimensions {{
                            date
                            query 
                        }}
                    }}
                }}
            }}
        }}
    """

    headers = { "Content-Type": "application/json", "Authorization": f"Bearer {cf_api_token}" }

    try:
        response = requests.post(api_url, headers=headers, json={'query': query})
        response.raise_for_status()
        # print(response.json())
        return jsonify(response.json())
    except requests.exceptions.RequestException as e:
        print(f"Error fetching D1 queries: {e}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(port=3000, debug=True)

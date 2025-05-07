import requests
import pymysql
from datetime import datetime
import math
import os

# API 요청 함수


def fetch_cultural_events(api_key, start_index, end_index):
    url = "http://openapi.seoul.go.kr:8088"
    endpoint = f"{url}/{api_key}/json/culturalEventInfo/{start_index}/{end_index}"

    try:
        response = requests.get(endpoint)
        response.raise_for_status()

        data = response.json()
        return data['culturalEventInfo']['row']
    except Exception as e:
        print(f"API 요청 중 오류 발생 (인덱스 {start_index}-{end_index}): {e}")
        return []


def initialize_categories_and_districts_if_empty(cursor):
    """
    카테고리와 행정구역 테이블이 비어있을 때만 초기 데이터를 삽입합니다.
    """
    # 카테고리 테이블 확인
    cursor.execute("SELECT COUNT(*) FROM categories")
    category_count = cursor.fetchone()[0]

    # 행정구역 테이블 확인
    cursor.execute("SELECT COUNT(*) FROM districts")
    district_count = cursor.fetchone()[0]

    current_time = datetime.now()

    # 카테고리가 없을 경우만 초기 데이터 삽입
    if category_count == 0:
        # 미리 정의된 카테고리 (가나다 순)
        categories = [
            (1, '교육/체험'),
            (2, '국악'),
            (3, '독주/독창회'),
            (4, '뮤지컬/오페라'),
            (5, '무용'),
            (6, '연극'),
            (7, '영화'),
            (8, '전시/미술'),
            (9, '축제-기타'),
            (10, '축제-문화/예술'),
            (11, '축제-시민화합'),
            (12, '축제-자연/경관'),
            (13, '축제-전통/역사'),
            (14, '클래식'),
            (15, '콘서트'),
            (16, '기타'),
        ]

        for category_id, name in categories:
            cursor.execute(
                "INSERT INTO categories (id, name, created_at) VALUES (%s, %s, %s)",
                (category_id, name, current_time)
            )
        print("카테고리 초기 데이터가 생성되었습니다.")

    # 행정구역이 없을 경우만 초기 데이터 삽입
    if district_count == 0:
        # 미리 정의된 행정구역 (가나다 순)
        districts = [
            (1, '강남구'),
            (2, '강동구'),
            (3, '강북구'),
            (4, '강서구'),
            (5, '관악구'),
            (6, '광진구'),
            (7, '구로구'),
            (8, '금천구'),
            (9, '노원구'),
            (10, '도봉구'),
            (11, '동대문구'),
            (12, '동작구'),
            (13, '마포구'),
            (14, '서대문구'),
            (15, '서초구'),
            (16, '성동구'),
            (17, '성북구'),
            (18, '송파구'),
            (19, '양천구'),
            (20, '영등포구'),
            (21, '용산구'),
            (22, '은평구'),
            (23, '종로구'),
            (24, '중구'),
            (25, '중랑구'),
            (26, '기타')
        ]

        for district_id, name in districts:
            cursor.execute(
                "INSERT INTO districts (id, name, created_at) VALUES (%s, %s, %s)",
                (district_id, name, current_time)
            )
        print("행정구역 초기 데이터가 생성되었습니다.")

# 카테고리 ID 가져오기 (이름에 따라 매핑)


def get_category_id(cursor, category_name):
    if not category_name or category_name.strip() == '':
        category_name = '기타'

    # 카테고리 이름과 ID 매핑 (필요시 업데이트)
    category_map = {
        '교육/체험': 1,
        '국악': 2,
        '독주/독창회': 3,
        '뮤지컬/오페라': 4,
        '무용': 5,
        '연극': 6,
        '영화': 7,
        '전시/미술': 8,
        '축제-기타': 9,
        '축제-문화/예술': 10,
        '축제-시민화합': 11,
        '축제-자연/경관': 12,
        '축제-전통/역사': 13,
        '클래식': 14,
        '콘서트': 15,
        '기타': 16,
    }

    # 매핑에 있는 카테고리는 해당 ID 반환
    if category_name in category_map:
        return category_map[category_name]

    # 매핑에 없는 카테고리는 DB에서 조회 후 없으면 생성
    query = "SELECT id FROM categories WHERE name = %s"
    cursor.execute(query, (category_name,))
    result = cursor.fetchone()

    if result:
        return result[0]

    # 새 카테고리 생성 - 매핑에 없는 새로운 카테고리는 ID 자동 생성
    current_time = datetime.now()
    insert_query = "INSERT INTO categories (name, created_at) VALUES (%s, %s)"
    cursor.execute(insert_query, (category_name, current_time))
    return cursor.lastrowid

# 행정구 ID 가져오기 (이름에 따라 매핑)


def get_district_id(cursor, district_name):
    if not district_name or district_name.strip() == '':
        district_name = '기타'

    # 행정구역 이름과 ID 매핑
    district_map = {
        '강남구': 1,
        '강동구': 2,
        '강북구': 3,
        '강서구': 4,
        '관악구': 5,
        '광진구': 6,
        '구로구': 7,
        '금천구': 8,
        '노원구': 9,
        '도봉구': 10,
        '동대문구': 11,
        '동작구': 12,
        '마포구': 13,
        '서대문구': 14,
        '서초구': 15,
        '성동구': 16,
        '성북구': 17,
        '송파구': 18,
        '양천구': 19,
        '영등포구': 20,
        '용산구': 21,
        '은평구': 22,
        '종로구': 23,
        '중구': 24,
        '중랑구': 25,
        '기타': 26
    }

    # 매핑에 있는 행정구역은 해당 ID 반환
    if district_name in district_map:
        return district_map[district_name]

    # 매핑에 없는 행정구역은 DB에서 조회 후 없으면 생성
    query = "SELECT id FROM districts WHERE name = %s"
    cursor.execute(query, (district_name,))
    result = cursor.fetchone()

    if result:
        return result[0]

    # 새 행정구역 생성 - 매핑에 없는 새로운 행정구역은 ID 자동 생성
    current_time = datetime.now()
    insert_query = "INSERT INTO districts (name, created_at) VALUES (%s, %s)"
    cursor.execute(insert_query, (district_name, current_time))
    return cursor.lastrowid

# 문화행사 삽입 또는 업데이트


def insert_or_update_event(cursor, event, category_id, district_id):
    # 동일 행사가 있는지 확인 (title과 start_date로 판단)
    check_query = """
    SELECT id FROM cultural_events 
    WHERE title = %s AND start_date = %s
    """

    start_date = parse_date(event.get('STRTDATE', ''))
    if not start_date:
        start_date = datetime.now()

    cursor.execute(check_query, (event.get('TITLE', ''), start_date))
    existing = cursor.fetchone()

    current_time = datetime.now()

    if existing:
        # 이미 존재하는 행사 업데이트
        update_query = """
        UPDATE cultural_events SET
            place = %s,
            org_name = %s,
            use_trgt = %s,
            use_fee = %s,
            player = %s,
            program = %s,
            etc_desc = %s,
            org_link = %s,
            main_img = %s,
            rgstdate = %s,
            ticket = %s,
            end_date = %s,
            themecode = %s,
            latitude = %s,
            longitude = %s,
            is_free = %s,
            hmpg_addr = %s,
            category_id = %s,
            district_id = %s,
            api_last_updated = %s,
            updated_at = %s
        WHERE id = %s
        """

        params = (
            event.get('PLACE', ''),
            event.get('ORG_NAME', ''),
            event.get('USE_TRGT', ''),
            event.get('USE_FEE', ''),
            event.get('PLAYER', ''),
            event.get('PROGRAM', ''),
            event.get('ETC_DESC', ''),
            event.get('ORG_LINK', ''),
            event.get('MAIN_IMG', ''),
            parse_date(event.get('RGSTDATE', '')),
            event.get('TICKET', ''),
            parse_date(event.get('END_DATE', '')),
            event.get('THEMECODE', ''),
            float(event.get('LOT', 0) or 0),
            float(event.get('LAT', 0) or 0),
            event.get('IS_FREE', ''),
            event.get('HMPG_ADDR', ''),
            category_id,
            district_id,
            current_time,  # api_last_updated
            current_time,  # updated_at
            existing[0]
        )

        cursor.execute(update_query, params)
        return existing[0], "updated"

    else:
        # 새 행사 삽입
        insert_query = """
        INSERT INTO cultural_events (
            title, place, org_name, use_trgt, use_fee, 
            player, program, etc_desc, org_link, main_img,
            rgstdate, ticket, start_date, end_date, themecode,
            latitude, longitude, is_free, hmpg_addr, 
            category_id, district_id, api_last_updated,
            created_at, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, 
            %s, %s, %s, %s, %s
        )
        """

        params = (
            event.get('TITLE', ''),
            event.get('PLACE', ''),
            event.get('ORG_NAME', ''),
            event.get('USE_TRGT', ''),
            event.get('USE_FEE', ''),
            event.get('PLAYER', ''),
            event.get('PROGRAM', ''),
            event.get('ETC_DESC', ''),
            event.get('ORG_LINK', ''),
            event.get('MAIN_IMG', ''),
            parse_date(event.get('RGSTDATE', '')),
            event.get('TICKET', ''),
            start_date,
            parse_date(event.get('END_DATE', '')),
            event.get('THEMECODE', ''),
            float(event.get('LOT', 0) or 0),
            float(event.get('LAT', 0) or 0),
            event.get('IS_FREE', ''),
            event.get('HMPG_ADDR', ''),
            category_id,
            district_id,
            current_time,  # api_last_updated
            current_time,  # created_at
            current_time   # updated_at
        )

        cursor.execute(insert_query, params)
        return cursor.lastrowid, "inserted"

# 날짜 문자열 파싱 함수


def parse_date(date_str):
    if not date_str or date_str.strip() == '':
        return None

    try:
        # API 응답의 날짜 형식에 맞게 파싱
        if 'T' in date_str:  # ISO 형식 (2025-03-28T14:30:00)
            return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        elif ' ' in date_str and '.' in date_str:  # 2025-05-08 00:00:00.0 형식
            # 밀리초 부분(.0)을 제거하고 처리
            clean_date_str = date_str.split('.')[0]
            return datetime.strptime(clean_date_str, '%Y-%m-%d %H:%M:%S')
        elif ' ' in date_str:  # 2025-05-08 00:00:00 형식
            return datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
        elif '-' in date_str:  # YYYY-MM-DD 형식
            return datetime.strptime(date_str, '%Y-%m-%d')
        else:  # YYYYMMDD 형식
            return datetime.strptime(date_str, '%Y%m%d')
    except ValueError:
        # print(f"날짜 파싱 오류: {date_str}")
        return None

# 메인 함수


def main():
    # 설정
    API_KEY = os.getenv('SEOUL_API_KEY')  # 서울시 공공데이터 포털에서 발급받은 API 키로 변경
    DB_CONFIG = {
        'host': os.getenv('DB_HOST'),
        'user': os.getenv('DB_USER'),  # 설정한 user로 변경
        'password': os.getenv('DB_PASSWORD'),  # 설정한 비밀번호로 변경
        'db': os.getenv('DB_NAME'),  # 설정한 db로 변경
        'charset': 'utf8mb4'
    }

    # 환경 변수 디버깅
    print(
        f"환경 변수 확인: DB_HOST={os.getenv('DB_HOST')}, DB_USER={os.getenv('DB_USER')}, DB_NAME={os.getenv('DB_NAME')}, API_KEY={API_KEY != None}")

    # 데이터 가져올 인덱스 범위
    START_INDEX = 1
    END_INDEX = 1000  # 원하는 최종 인덱스로 변경, 예: 5000

    # 배치 사이즈 설정 (API 최대 허용: 1000)
    BATCH_SIZE = 1000

    # 전체 레코드 수
    total_records = END_INDEX - START_INDEX + 1

    # 배치 수 계산
    num_batches = math.ceil(total_records / BATCH_SIZE)

    print(f"총 {total_records}개 레코드를 {num_batches}개 배치로 처리합니다. (배치 크기: {BATCH_SIZE})")

    # 데이터베이스 연결
    connection = pymysql.connect(**DB_CONFIG)

    try:
        with connection.cursor() as cursor:
            # 카테고리와 행정구역 데이터 초기화 (비어있을 때만)
            initialize_categories_and_districts_if_empty(cursor)
            connection.commit()

            # 결과 통계
            total_inserted = 0
            total_updated = 0
            total_processed = 0

            # 배치 처리
            for batch in range(num_batches):
                batch_start = START_INDEX + (batch * BATCH_SIZE)
                batch_end = min(START_INDEX + ((batch + 1)
                                * BATCH_SIZE) - 1, END_INDEX)

                print(
                    f"배치 {batch + 1}/{num_batches} 처리 중 (인덱스: {batch_start}-{batch_end})...")

                # API 요청 및 데이터 가져오기
                events = fetch_cultural_events(API_KEY, batch_start, batch_end)

                if not events:
                    print(f"배치 {batch + 1}: 가져온 행사 정보가 없습니다.")
                    continue

                # 각 행사 처리
                batch_results = []

                for event in events:
                    try:
                        # 카테고리 ID 가져오기
                        category_name = event.get('CODENAME', '기타')
                        category_id = get_category_id(cursor, category_name)

                        # 행정구역 ID 가져오기
                        district_name = event.get('GUNAME', '기타')
                        district_id = get_district_id(cursor, district_name)

                        # 행사 삽입 또는 업데이트
                        event_id, status = insert_or_update_event(
                            cursor, event, category_id, district_id)

                        batch_results.append({
                            'id': event_id,
                            'title': event.get('TITLE', ''),
                            'status': status
                        })

                    except Exception as e:
                        print(f"행사 처리 중 오류 발생: {e}")

                # 배치 결과 커밋
                connection.commit()

                # 배치 결과 통계
                batch_inserted = sum(
                    1 for r in batch_results if r['status'] == 'inserted')
                batch_updated = sum(
                    1 for r in batch_results if r['status'] == 'updated')

                print(f"배치 {batch + 1} 결과: 총 {len(batch_results)}개 행사 처리")
                print(f"- 삽입: {batch_inserted}개")
                print(f"- 업데이트: {batch_updated}개")

                # 전체 통계 업데이트
                total_inserted += batch_inserted
                total_updated += batch_updated
                total_processed += len(batch_results)

            # 최종 결과 출력
            print("\n--- 최종 처리 결과 ---")
            print(f"총 처리된 행사: {total_processed}개")
            print(f"- 새로 삽입: {total_inserted}개")
            print(f"- 업데이트: {total_updated}개")

    except Exception as e:
        print(f"데이터베이스 작업 중 오류 발생: {e}")
        connection.rollback()
    finally:
        connection.close()


if __name__ == "__main__":
    main()

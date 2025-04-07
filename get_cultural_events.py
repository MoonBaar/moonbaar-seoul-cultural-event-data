import requests
import pymysql
from datetime import datetime
import math

# 테이블 생성 함수


def create_tables_if_not_exist(cursor):
    # 카테고리 테이블
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS categories (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      name VARCHAR(50) UNIQUE,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    # 행정구 테이블
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS districts (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      name VARCHAR(50) UNIQUE,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    # 문화행사 테이블
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS cultural_events (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      title VARCHAR(255),
      place VARCHAR(255),
      org_name VARCHAR(255),
      use_trgt VARCHAR(255),
      use_fee VARCHAR(255),
      player TEXT,
      program TEXT,
      etc_desc TEXT,
      org_link TEXT,
      main_img VARCHAR(500),
      rgstdate DATE,
      ticket VARCHAR(50),
      start_date TIMESTAMP,
      end_date TIMESTAMP,
      themecode VARCHAR(100),
      latitude DECIMAL(10,7),
      longitude DECIMAL(10,7),
      is_free VARCHAR(10),
      hmpg_addr TEXT,
      category_id BIGINT,
      district_id BIGINT,
      api_last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      FOREIGN KEY (category_id) REFERENCES categories(id),
      FOREIGN KEY (district_id) REFERENCES districts(id),
      UNIQUE KEY (title, start_date)
    )
    """)

    # 사용자 테이블 (참조를 위해 필요)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS users (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      oauth_id VARCHAR(255) UNIQUE,
      oauth_provider VARCHAR(50),
      nickname VARCHAR(100),
      profile_image_url VARCHAR(255),
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
    """)

    # 방문 기록 테이블
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS visits (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      user_id BIGINT,
      event_id BIGINT,
      visited_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (user_id) REFERENCES users(id),
      FOREIGN KEY (event_id) REFERENCES cultural_events(id),
      UNIQUE KEY (user_id, event_id)
    )
    """)

    # 좋아요 테이블
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS liked_events (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      user_id BIGINT,
      event_id BIGINT,
      liked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (user_id) REFERENCES users(id),
      FOREIGN KEY (event_id) REFERENCES cultural_events(id),
      UNIQUE KEY (user_id, event_id)
    )
    """)

    # 배지 테이블
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS badges (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      badge_type VARCHAR(50),
      code VARCHAR(50) UNIQUE,
      name VARCHAR(100),
      description TEXT,
      image_url VARCHAR(255),
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
    """)

    # 사용자 배지 테이블
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS user_badges (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      user_id BIGINT,
      badge_id BIGINT,
      acquired_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (user_id) REFERENCES users(id),
      FOREIGN KEY (badge_id) REFERENCES badges(id),
      UNIQUE KEY (user_id, badge_id)
    )
    """)

    print("필요한 테이블이 모두 생성되었습니다.")

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

# 카테고리 ID 가져오기 (없으면 생성)


def get_or_create_category(cursor, category_name):
    if not category_name or category_name.strip() == '':
        category_name = '기타'

    query = "SELECT id FROM categories WHERE name = %s"
    cursor.execute(query, (category_name,))
    result = cursor.fetchone()

    if result:
        return result[0]

    # 새 카테고리 생성
    insert_query = "INSERT INTO categories (name) VALUES (%s)"
    cursor.execute(insert_query, (category_name,))
    return cursor.lastrowid

# 행정구 ID 가져오기 (없으면 생성)


def get_or_create_district(cursor, district_name):
    if not district_name or district_name.strip() == '':
        district_name = '기타'

    query = "SELECT id FROM districts WHERE name = %s"
    cursor.execute(query, (district_name,))
    result = cursor.fetchone()

    if result:
        return result[0]

    # 새 행정구 생성
    insert_query = "INSERT INTO districts (name) VALUES (%s)"
    cursor.execute(insert_query, (district_name,))
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
            api_last_updated = NOW(),
            updated_at = NOW()
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
            %s, %s, NOW(), NOW(), NOW()
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
            district_id
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
    API_KEY = "API_KEY"  # 서울시 공공데이터 포털에서 발급받은 API 키로 변경
    DB_CONFIG = {
        'host': 'localhost',
        'user': 'USER',  # 설정한 user로 변경
        'password': 'PASSWORD',  # 설정한 비밀번호로 변경
        'db': 'DB',  # 설정한 db로 변경
        'charset': 'utf8mb4'
    }

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
            # 필요한 테이블 생성
            create_tables_if_not_exist(cursor)

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
                        # 카테고리 확인 및 생성
                        category_name = event.get('CODENAME', '기타')
                        category_id = get_or_create_category(
                            cursor, category_name)

                        # 행정구역 확인 및 생성
                        district_name = event.get('GUNAME', '기타')
                        district_id = get_or_create_district(
                            cursor, district_name)

                        # 행사 삽입 또는 업데이트
                        event_id, status = insert_or_update_event(
                            cursor, event, category_id, district_id
                        )

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

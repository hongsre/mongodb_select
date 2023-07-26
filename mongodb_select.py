import os
from datetime import datetime
import pytz
from pymongo import MongoClient
from json import JSONEncoder
from bson import ObjectId
from bson.binary import Binary
from multiprocessing import Pool
import pandas as pd
import logging

mongo_info = [
    {
        'host': 'db host',
        'db': '',
        'username': '',
        'password': '',
        'authSource': ''
    }
]

# 여기에 실제 컬렉션 이름을 추가하세요.
collection_names = [
    'collection1', 
    'collection2', 
    'collection3'
    ]

# 추가 필터링 조건
additional_filter = {
    '조건': "값"  # int, string, date 등 포맷을 꼭 입력해줘야합니다. 
}

# 시간 범위를 지정합니다. KST (예: "2023-06-06 00:00:00")
# 지정하지 않을 경우 전체 데이터 조회를 진행합니다.
start_datetime_str = "2022-10-12 00:00:00"  # 여기에 시작 시간을 넣으세요. 지정하지 않으려면 "" 값으로 변경해주세요
end_datetime_str = "2022-10-13 20:59:59"  # 여기에 종료 시간을 넣으세요. 지정하지 않으려면 "" 값으로 변경해주세요

# 범위를 정의합니다. (예: 1일을 나타내는 86400초로 설정 5분 300, 10분 600, 30분 1800 1시간 3600 )
range_ts_u = 3600

# 현재 시간 가져오기
now = datetime.now()

# 주어진 형식으로 시간 변환
time_str = now.strftime("%Y-%m-%d_%H%M%S")

# datetime 객체를 KST 시간대로 설정합니다.
kst = pytz.timezone('Asia/Seoul')

class JSONEncoderCustom(JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        if isinstance(o, datetime):
            return o.isoformat()
        if isinstance(o, bytes):
            return str(o)
        if isinstance(o, int):
            return str(o)
        if isinstance(o, float):
            return str(o)
        if isinstance(o, Binary):
            return str(o)
        return JSONEncoder.default(self, o)

def get_data_from_host(args):
    now = datetime.now()
    try:
        info, collection_names = args
        client = MongoClient(
            f"mongodb://{info['host']}",
            username=info['username'],
            password=info['password'],
            authSource=info['authSource'],
            directConnection=True
        )
        db = client[info['db']]  # 여기에 데이터베이스 이름을 넣으세요.

        # 결과를 저장할 폴더를 생성합니다.
        folder_name = info['host'].split(':')[0]

        # 로그 파일을 설정합니다. 여기서는 병렬 처리에 대한 로그를 기록하는 로그 파일을 설정하였습니다.
        if not os.path.exists(f'results'):
            os.makedirs('results')

        if not os.path.exists(f'logs'):
            os.makedirs('logs')

        logging.basicConfig(filename=f'logs/{folder_name}.log', level=logging.INFO, format='%(asctime)s %(message)s')

        # 만약 콘솔에도 로그 메시지를 출력하고 싶다면 다음 코드를 추가하세요.
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s %(message)s')
        console.setFormatter(formatter)
        logging.getLogger('').addHandler(console)
        if not os.path.exists(f'results/{folder_name}'):
            os.makedirs(f'results/{folder_name}')

        # 각 컬렉션에 대해 데이터를 추출합니다.
        total_count = len(collection_names)
        count = 0
        for collection_name in collection_names:
            collection = db[collection_name]
            result_list = []
            # 전체 문서의 개수를 확인합니다.
            total_docs = collection.count_documents({})
            if total_docs == 0:
                logging.info(f"{folder_name} No documents in {collection_name}. Skipping this collection. ({count}/{total_count})")
                continue

            min_ts_id = collection.find().sort([('_id', 1)]).limit(1)[0]['_id']
            min_ts_u = int(min_ts_id.generation_time.timestamp())
            max_ts_id = collection.find().sort([('_id', -1)]).limit(1)[0]['_id']
            max_ts_u = int(max_ts_id.generation_time.timestamp())

            if start_datetime_str:
                start_datetime = datetime.strptime(start_datetime_str, "%Y-%m-%d %H:%M:%S")
                start_datetime = kst.localize(start_datetime)
                start_datetime_utc = start_datetime.astimezone(pytz.utc)
                min_ts_u = int(start_datetime_utc.timestamp())
            
            if end_datetime_str: 
                end_datetime = datetime.strptime(end_datetime_str, "%Y-%m-%d %H:%M:%S")
                end_datetime = kst.localize(end_datetime)
                end_datetime_utc = end_datetime.astimezone(pytz.utc)
                max_ts_u = int(end_datetime_utc.timestamp())

            # 범위에 따라 쿼리를 실행하고 데이터를 추출합니다.
            # 시작 시간을 로깅합니다.
            logging.info(f"{folder_name} Start process for collection: {collection_name} ({count}/{total_count})")

            for ts_u in range(min_ts_u, max_ts_u, range_ts_u):
                start_unix_time = ts_u
                end_unix_time = ts_u + range_ts_u
                start_hex_ts_u = f'{start_unix_time:0>8x}0000000000000000'
                end_hex_ts_u = f'{end_unix_time:0>8x}0000000000000000'
                start_object_id = ObjectId(start_hex_ts_u)
                end_object_id = ObjectId(end_hex_ts_u)
                # _id 범위와 조건에 맞는 데이터를 찾습니다.
                query = {
                    '_id': {'$gte': start_object_id, '$lt': end_object_id}
                }
                # 추가 필터링 조건을 쿼리에 적용합니다.
                query.update(additional_filter)
                result = collection.find(query)
                result_list.extend(list(result))

            if result_list:
                for item in result_list:
                    for key, value in item.items():
                        if isinstance(value, (ObjectId, datetime, bytes, int, float, Binary)):
                            item[key] = str(value)

            # 결과 데이터를 pandas DataFrame 객체로 변환합니다.
            df = pd.DataFrame(result_list)

            # DataFrame 객체를 CSV 파일로 저장합니다.
            df.to_excel(f'results/{folder_name}/{collection_name}_result_{time_str}.xlsx', engine='openpyxl', index=False)
            count += 1
            logging.info(f"{folder_name} done process for collection: {collection_name} ({count}/{total_count})")
    except Exception as e:
        logging.error(f"{folder_name} Error while processing query: {e}")
        logging.error(f"{folder_name} Failed to process query Moving on to next query.")
    end = datetime.now()
    diff = (end - now).total_seconds()
    hours = int(diff // 3600)  # 시간 계산
    minutes = int((diff % 3600) // 60)  # 분 계산
    seconds = int(diff % 60)  # 초 계산

    time_difference = ""
    if hours != 0:
        time_difference += f"{hours}hour "
    if minutes != 0:
        time_difference += f"{minutes}min "
    if seconds != 0 or (hours == 0 and minutes == 0):
        time_difference += f"{seconds}sec"

    logging.info(f"{folder_name} Select completed. Duration : ({time_difference})")


if __name__ == "__main__":
    with Pool() as p:
        p.map(get_data_from_host, [(info, collection_names) for info in mongo_info])
        p.close()
        p.join()

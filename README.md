# mongodb 조회용 python 스크립트입니다.
```
python = "^3.11"
pytz = "^2023.3"
pandas = "^2.0.3"
pymongo = "^4.4.1"
openpyxl = "^3.1.2"
```

### 각 몽고DB 호스트 별로 collection 조회를 진행합니다.
```
mongo_info = [ 
    {
        'host': 'db host',
        'db': '',
        'username': '',
        'password': '',
        'authSource': ''
    }
]
```

### 자원 사용량의 문제가 있을 수 있어, 조회하는 컬랙션에 대해서는 순차 처리됩니다.
```
collection_names = [
    'collection1', 
    'collection2', 
    'collection3'
    ]
```
### 추가 필터링 조건
```
additional_filter = {
    '조건': "값"  # int, string, date 등 포맷을 꼭 입력해줘야합니다. 
}
```

### 시간 범위를 지정합니다. KST (예: "2023-06-06 00:00:00")
### 지정하지 않을 경우 전체 데이터 조회를 진행합니다.
```
start_datetime_str = "2022-10-12 00:00:00"  
end_datetime_str = "2022-10-13 20:59:59"  
```
### 범위를 정의합니다. (예: 1일을 나타내는 86400초로 설정 5분 300, 10분 600, 30분 1800 1시간 3600 )
```
range_ts_u = 3600
```

### 위 설정을 완료하면 아래 명령어로 실행해 주시면됩니다.
```
python mongodb_select.py
```

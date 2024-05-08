import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import glob
import os


path = './'
# 특정 경로 설정
#path = 'C:\\Users-----\\Data\\'  # 여기에 엑셀 파일이 있는 디렉토리 경로를 입력하세요.
# path = 'C:\\Users-------\\mig'  # 여기에 엑셀 파일이 있는 디렉토리 경로를 입력하세요.
# path = '.\\target'
print(path)
database_url = ''
engine = create_engine(
    database_url,
    connect_args={'options': '-csearch_path=skins'}
)
xlsx_files = glob.glob(os.path.join(path, '*.xlsx'))
print(xlsx_files)
idx = 0
for file_path in xlsx_files:
    # 엑셀 파일의 모든 시트를 읽고 각 시트를 데이터베이스에 테이블로 저장
    xls = pd.ExcelFile(file_path)
    for sheet_name in xls.sheet_names:
        df = pd.read_excel(xls, sheet_name=sheet_name)

        # 데이터프레임의 컬럼 이름에서 공백 제거
        df.columns = [str(c).strip() for c in df.columns]

        # 테이블 이름 정의 (파일 이름 + 시트 이름)
        # table_name = f"{os.path.splitext(os.path.basename(file_path))[0]}_{sheet_name}".lower()
        # table_name = f"{os.path.splitext(os.path.basename(file_path))[0]}".lower()
        # file_name = f"{os.path.splitext(os.path.basename(file_path))[0]}".lower()
        file_name = 'location'
        sheet_name = f"{sheet_name}".lower()
        table_name = file_name if file_name == sheet_name else f"{file_name}_{sheet_name}"


        #우선 테이블 명만 print
        print(str(idx) + '\t' + file_name + '\t' + table_name)

        idx = idx + 1

        # PostgreSQL 테이블 생성 및 데이터 삽입
        df.to_sql('20240530_'+table_name, engine, if_exists='replace', index=False)



# sql 관련 정보 추가
# sql reader
# 행정구역코드 -> id
# 구분 -> country
# 1단계 -> level1
# 2단계 -> level2
# 3단계 -> level3
# 격자X -> gridX
# 격자Y -> gridY
# 경도(시) -> lon_hour
# 경도(분) -> lon_min
# 경도(초) -> lon_sec
# 위도(시) -> lat_hour
# 위도(분) -> lat_min
# 위도(초) -> lat_sec
# 경도(초1/100) -> longitude
# 위도(초1/100) -> latitude


# CREATE TABLE skins._20240530_city_location (
# 	id bigserial NOT NULL,
# 	country varchar(8) NULL,
# 	level1 varchar(50) NULL,
# 	level2 varchar(50) NULL,
# 	level3 varchar(50) NULL,
# 	gridX int8 NULL,
# 	gridY int8 NULL,
# 	lon_hour int8 NULL,
# 	lon_min int8 NULL,
# 	lon_sec float8 NULL,
# 	lat_hour int8 NULL,
# 	lat_min int8 NULL,
# 	lat_sec float8 NULL,
# 	longitude float8 NULL,
# 	latitude float8 NULL,
# 	CONSTRAINT _20240530_city_location PRIMARY KEY (id)
# );


# CREATE INDEX _20240529_tb_consult_info_consult_date_idx ON skins.tb_consult_info USING btree (consult_date);
# CREATE INDEX _20240529_tb_consult_info_userkey_idx ON skins.tb_consult_info USING btree (userkey, consult_date);


# insert into skins._20240529_test
# select
# userkey , centercd , cstmid , ucstmid , name, phone, birthdate , birthcd , sex , usertype , memo, comment, email, nat_cd , ra_cd , create_dt , upddate 
# from skins.zz_20240529_fgt_rawdata_tbmembers zt ;
	
# select * from skins._20240529_test ;	
	
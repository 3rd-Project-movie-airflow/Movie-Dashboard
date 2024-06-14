# 1. 프로젝트 개요

## 프로젝트 주제

영화진흥위원회 API를 이용한 End-to-end 데이터 파이프라인 및 대시보드 구성

## 프로젝트 목표

1. 공동 작업을 위한 프로젝트 환경 구성
2. API 호출을 통해 얻은 데이터 전처리 및 데이터 웨어하우스 적재
3. 일간/주간 업데이트를 위한 Airflow DAG 작성
4. Preset을 이용한 차트 생성 및 대시보드 구성

# 2. 프로세스 설명

### 공동 작업을 위한 프로젝트 환경 구성

![Untitled](Image_folder/Untitled.png)

### Github Actions 사용하여 클라우드 컴포저의 Dags 파일과 연동

1. 클라우드 컴포저의 IAM에서 서비스 계정 생성 >> 적당한 역할 부여 
    - 이 과정에서 역할에 대한 오류가 발생하여 가장 높은 등급인 소유자 역할 부여
    - 실무 환경에서는 보안 이슈가 존재한다고 생각하며 역할에 대한 공부를 통해 최소한의 역할을 부여하는 것이 좋다고 생각한다.
2. 서비스 계정을 선택하고 키 생성
    - json 형식으로 생성하고 이 키를 5번의 변수에 저장해야함
3. 구글 클라우드 스토리지에서 서비스 계정에 대한 접근 권한 설정
4. Github Action의 yml파일에서 사용될 변수들을 시크릿으로 저장
**Repositories**의 설정 >> 왼쪽 목록에서  **secrets and variables** >> **Actions**에 변수로 저장
사용 변수
    - GCP_REGION (클라우드 컴포저의 위치 us-***), 
    - GCP_PROJECT (내 프로젝트의 id ‘My First Project’가 아닌 id 존재 avd-csa-***-n3 )
    - GCP_KEY (2번에서 생성한 json 파일 그대로 입력)
5. Github의 루트 폴더에 .github/workflows/ 생성 후 workflow 폴더 아래에 
Github Actions가 실행될 yml 파일 작성

    airflow/dags 폴더의 변경사항이 있을 때 airflow/dags 폴더와 클라우드 스토리지 미러링
    

### API 호출을 통해 얻은 데이터 전처리 및 데이터 웨어하우스 적재

- API
    
    [영화진흥위원회 오픈API](https://www.kobis.or.kr/kobisopenapi/homepg/apiservice/searchServiceInfo.do)
    
    [검색 > 뉴스 - Search API](https://developers.naver.com/docs/serviceapi/search/news/news.md)
    
- 크롤링
    
    [KOFIC 영화관 입장권 통합전산망 :: 일별 좌석점유율](https://www.kobis.or.kr/kobis/business/stat/boxs/findDailySeatTicketList.do)
    
    [KOFIC 영화관 입장권 통합전산망 :: 지역별 영화상영관현황](https://www.kobis.or.kr/kobis/business/mast/thea/findAreaTheaterStat.do)
    

- 데이터 전처리(ETL)
    
    API 호출 → 파이썬 코드로 데이터 가공 → Redshift 테이블 적재
    
- 데이터 테이블
    
    ![3차 프로젝트 ERD.drawio.png](Image_folder/3%25E1%2584%258E%25E1%2585%25A1_%25E1%2584%2591%25E1%2585%25B3%25E1%2584%2585%25E1%2585%25A9%25E1%2584%258C%25E1%2585%25A6%25E1%2586%25A8%25E1%2584%2590%25E1%2585%25B3_ERD.drawio.png)
    

### 일간/주간 업데이트를 위한 Airflow DAG 작성

![Untitled](Image_folder/Untitled%201.png)

- boxoffice_news
    - 매일 12시 00분 하루전 boxoffice top10 영화의 네이버 뉴스 링크
- daily_box_office_audicnt
    - 매일 2시 00분 일자별 영화 관객수
- daily_seat_ratio
    - 매일 2시 00분 일자별 좌석점유율 크롤링
- Movie_Area_Cinema_Count
    - 매일 1시 00분 지역별 영화관 개수 크롤링
- Movie_Count
    - 매주 월요일 1시 00분 연도별 개봉한 영화의 수 업데이트
- Movie_Genre_Count
    - 매일 1시 00분 2024년 개봉한 영화의 장르 업데이트
- update_actor_audiCnt
    - 매일 12시 00분 하루전 boxoffice top10 영화의 배우별 관객 수 업데이트
- update_boxoffice_audiCnt
    - 매일 12시 00분 하루전 boxoffice top10 영화의 관객 수 업데이트
- weekly_region_audience_top10
    - 매주 월요일 0시 10분 지역별 top10영화의 개별 관객수
- weekly_region_sales_top10SUM
    - 매주 월요일 0시 10분 지역별 top10영화의 총 매출 합(주간매출)
    - top 10 영화가 지역마다 다를 가능성이 있다는 점을 배제하고 만듦.
        
### Preset을 이용한 차트 생성 및 대시보드 구성

![영화-2024-06-13T07-11-14.048Z.jpg](Image_folder/%25E1%2584%258B%25E1%2585%25A7%25E1%2586%25BC%25E1%2584%2592%25E1%2585%25AA-2024-06-13T07-11-14.048Z.jpg)

대시보드 위쪽은 박스오피스, 관객수, 개봉영화추이 아래쪽은 지역별 데이터를 나타냄

**어떤 테이블을 이용해서 만들었는지 간단히 작성**

- 오늘의 박스오피스 순위
    
    boxoffice_news 테이블
    
- 2024 영화별 관객수 TOP10
    
    boxoffice_audicnt 테이블의 audi_cnt를 내림차순으로 10개 나열
    
- 2024 개봉 영화 장르별 파이차트 (매일 갱신)
    
    genre_2024 테이블
    
    개봉한 영화를 장르별로 카운트하여 파이차트로 생성
    
- 2014 ~ 2024 개봉 영화 수 추이 (매주 갱신)
    
    movie_cnt 테이블
    
- 일별 좌석 점유율과 관람객
    
    daily_seat_ratio테이블을 기준으로 daily_box_office_audicnt테이블을 조인시켜 일자별 좌석 점유율 top7의 데이터를 나타냄
    
- 지역별 top10 매출액 합 (주마다 갱신)
    
    weekly_region_sales_top10SUM dag를 이용
    
    진한 파란색일수록 매출액이 많고, 진한 핑크일수록 매출액이 적음
    
    생각보다 경남 지역에서 매출이 높은 것을 알 수 있음 
    
- 지역별 영화순위별 관람객수 (주마다 갱신)
    
    weekly_region_audience_top10 dag이용
    
    관람객 수의 비율을 쉽게 확인할 수 있고, 1,2,3위 순서대로 아래서부터 stack
    
    서울 경기에 대부분의 관람객이 몰려있어, 다른 지역들의 관람객 수가 적다는 것을 알 수 있음.
    
- 지도 차트로 나타낸 지역별 영화관 수 비교
    
    area_cinema 테이블과 area_codes 테이블 조인
    
    진한 초록색일 수록 영화관 수가 많은 지역
    
- 지역별 영화관 수 막대 그래프 비교
    
    area_cinema 테이블과 area_codes 테이블 조인
    
- 2024 배우별 관객수 TOP10
    
    actor_audicnt 테이블의 audi_cnt를 내림차순으로 10개 나열
    

# 3. 팀원 및 역할

| 팀원 | 역할 |
| --- | --- |
| 남원우 |  DAG 작성 및 차트 구성, 보고서 초안, Github Actions 설정 |
| 손봉호(팀장) | 프로젝트 환경 구성(GCP), DAG 작성 및 차트 구성, Github Action 설정 |
| 이정화 |  DAG 작성 및 차트 구성,데이터 마트 생성, PPT 생성, 보고서작성 |
| 정가인 | DAG 작성 및 차트 구성, 보고서 초안, PPT 생성 |

# 4. 활용 기술

![Untitled](Image_folder/Untitled%202.png)

| 데이터 파이프라인 | Airflow |
| --- | --- |
| 데이터 웨어하우스 | Redshift |
| 데이터 시각화  | Preset |
| 데이터 처리 | Python - requests, bs4, pandas |
| CI / CD | Git Action |
| 협업 툴 | GCP, Git, Slack, Notion, Gather, |

# 5. 프로젝트 결과

**영화진흥위원회 데이터를 활용한 대시보드**

![영화-2024-06-13T07-11-14.048Z.jpg](Image_folder/bdf9cec0-c9c1-44eb-bd3a-7f41b50796d6.png)

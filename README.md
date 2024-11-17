# spark-log-processor

## 개요
### 목적
> `spark-log-processor`는 사용자 Activity 로그를 수집, 처리 및 적재하기 위한 프로그램으로, Apache Spark를 기반으로 동작합니다. 사용자는 CSV 파일 형식의 로그 데이터를 처리하고, Parquet 형식으로 변환하여 Spark External Table로 적재할 수 있습니다. 이 프로그램은 Hive Metastore와 연동하여 데이터를 관리합니다.

### 대상 데이터
[Kaggle - 이커머스 행동 데이터](https://www.kaggle.com/mkechinov/ecommerce-behavior-data-from-multi-category-store)

### 지원 포맷
- **입력 포맷**: CSV
- **입력 위치**: Spark에서 접근 가능한 File System
- **출력 포맷**: Spark External Table (Delta Lake)
- **출력 위치**: S3

### 주요 기능
- 시간대 변환 및 파티셔닝: `event_time` 컬럼을 서울 시간대(KST)로 변환하여 파티셔닝 컬럼으로 활용
- Parquet 저장: 데이터는 Parquet 형식으로 저장되며, Snappy 압축 적용
- Hive 연동: 저장된 Parquet 파일은 Hive를 통해 관리되는 Spark External Table로 적재
- 동적 테이블 생성/적재: 테이블이 존재하지 않으면 새로 생성하고, 이미 존재하면 append 모드로 데이터를 추가

## 사용 방법
### 환경 구성
#### Spark 클러스터
Spark 클러스터는 기구축된 환경을 사용하는 것을 가정

#### Hive Meatstore Setup
> 기 구축된 Hive 환경을 사용할 경우 이 단계는 진행하지 않습니다.

아래 명령어를 통해 Docker 기반으로 Hive를 설정하고 실행
```shell
docker compose -f docker-hive/docker-compose.yml up -d --build
```

### 2. Configuration 파일 구성

#### Hive 설정 파일 (`hive-site.xml`)
1. Docker 기반의 Hive Metastore를 사용할 경우) 별도 설정이 필요 없음
2. 기 구축된 Hive 환경을 사용할 경우) 해당 Hive의 `hive-site.xml` 파일을 프로젝트의 `conf/` 경로에 배치

#### 애플리케이션 설정 파일 (`conf.properties`)
`conf.properties` 파일은 프로그램에서 사용할 주요 설정 항목들을 포함함. 이 파일을 통해 입력 파일 경로, Hive 테이블 이름 및 출력 경로를 지정 가능
- `inputCsvFile`: 처리할 입력 CSV 파일의 경로
- `targetTableName`: 데이터를 적재할 Hive External Table의 이름
- `outputPath`: Parquet 파일이 저장될 경로. S3 지원
- `maxRetries`: 작업 실패 시 최대 재시도 횟수 (default: 5)

#### AWS Credentials (`.aws_credentails`)
데이터 저장 경로로 AWS를 사용하는 경우 Credentail 정보 입력 필요
- `aws_access_key_id`: AWS에 접근할 때 사용하는 고유한 사용자 ID
- `aws_secret_access_key`: AWS에서 인증을 위해 사용하는 비밀 키. `aws_access_key_id`와 함께 사용됨

### 3. 프로젝트 빌드 및 결과물 이동
아래 명령어를 통해 프로젝트 빌드
```shell
gradlew clean build
```

빌드가 완료되면, 결과물은 Spark Driver Program 실행 노드의 아래 경로로 이동시킴
- `conf/*` -> `$SPARK_HOME/conf`
- `app/build/libs/app.jar` -> `$SPARK_HOME`


### 4. Spark Application 실행
아래 명령어로 Spark Application을 클러스터에 제출
```shell
$SPARK_HOME/bin/spark-submit --class com.example.App \
  --packages org.postgresql:postgresql:42.2.5,io.delta:delta-spark_2.12:3.2.1,org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901 \
  app.jar
```

### 5. 결과 확인
`Process Succeed !`가 출력되는 경우 프로그램이 정상적으로 완료됨
## Airflow Site 실행

### 1. 환경설정

```
1-1. AWS EC2 인스턴스 생성 후 Public IP 주소 복사
```

```
1-2. 로컬에서 C:\Users\user\.ssh 폴더에 있는 config 파일 내용 수정

> Host ec2
        HostName aws ec2 public ip입력
        User ubuntu
        IdentityFile C:/Users/user/.ssh/위치 안에 있는 aws pem key 경로 설정
```

### 2. VsCode에서 작업하기 

```
2-1. Extensions에서 Remote Explorer 설치 후 ubuntu 폴더 선택
```

```
2-2. Termianl 창에 접속확인 후, airflow 설치한 경로에서 `docker start -ai airlfow2.5` 실행
```

```
2-3. (Bash 안에서) nohup airlfow webserver & 후, nohup airflow scheduler & 실행
```

### 3. Airflow Site 들어가기

```
1-6. 크롬창에서 `aws ec2 public ip:8080` 으로 접속 후, airflow site 로그인
```

### 4. Airflow file 생성하기

```
terminal창에서 `cd airflow` → `파일이름명.py` → code 입력 → :wq 저장 → airflow site 새로고침 or nohup scheduler 실행
```

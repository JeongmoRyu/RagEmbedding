FROM python:3.8-slim

# 작업 디렉토리 설정
WORKDIR /workplace

# 애플리케이션 파일 복사
COPY . /workplace

# 필요 패키지 설치
# test동안 주석처리
RUN pip install --no-cache-dir -r requirements.txt

# 환경 변수 설정
ENV ENV=local
COPY cfg/local_cfg.yaml /workplace/cfg/local_cfg.yaml
COPY cfg/prd_cfg.yaml /workplace/cfg/prd_cfg.yaml
COPY cfg/dev_cfg.yaml /workplace/cfg/dev_cfg.yaml
# COPY cfg/qa_cfg.yaml /workplace/cfg/qa_cfg.yaml


# COPY cfg/local_cfg.yaml /workplace/cfg/local_cfg.yaml
# ENV CONFIG_PATH=/workplace/cfg/local_cfg.yaml

# 포트 노출
EXPOSE 5001
EXPOSE 5002


# 기본 명령어 설정
# ENTRYPOINT ["python", "src/console_es.py"]
ENTRYPOINT /bin/bash -c "if [ \"$ENV\" = 'prd' ]; then \
                           export CONFIG_PATH=/workplace/cfg/prd_cfg.yaml; \
                         elif [ \"$ENV\" = 'dev' ]; then \
                           export CONFIG_PATH=/workplace/cfg/dev_cfg.yaml; \
                         else \
                           export CONFIG_PATH=/workplace/cfg/local_cfg.yaml; \
                         fi && \
                         python src/console_es.py"
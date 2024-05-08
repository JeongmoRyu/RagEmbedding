FROM 883424753239.dkr.ecr.ap-northeast-2.amazonaws.com/apne2-devops-base-python3.8:85aa856fff2d89f8364e5e7588cd54223fbcf8ee-40571

# Copy function code
COPY . /workplace
USER root
RUN chmod 777 /workplace
WORKDIR /workplace

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 443

### vectorize
ENTRYPOINT ["python", "src/main.py"]
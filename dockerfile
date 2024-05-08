FROM 이미지
# Copy function code
COPY . /workplace
USER root
RUN chmod 777 /workplace
WORKDIR /workplace

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 443

### vectorize
ENTRYPOINT ["python", "src/main.py"]
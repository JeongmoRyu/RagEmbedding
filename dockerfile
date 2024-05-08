FROM ***
# Copy function code
COPY . /workplace
USER root
RUN chmod ***
WORKDIR /***

RUN pip install --no-cache-dir -r requirements.txt

# Copy the local_cfg.yml file to the container
COPY cfg/local_cfg.yml /workplace/cfg/local_cfg.yml

# Set the environment variable to point to the configuration file
ENV CONFIG_PATH=/workplace/cfg/local_cfg.yml

EXPOSE 443

# Define default command
ENTRYPOINT ["python", "src/main.py"]

# How to run

### 1. Create Virtual Environtment and Install Requirements
https://fastapi.tiangolo.com/virtual-environments/#create-a-virtual-environment

### 2. Create .env files containing:
```
KAFKA_BROKER = IP:Port for Kafka Broker
CONNECTION_STRING = MongoDB connection string

# For email alterting service, put your prevered email credentials and server
MAIL_USERNAME = 
MAIL_PASSWORD = 
MAIL_FROM = 
MAIL_PORT = 
MAIL_SERVER = 
```

### 3. Run FastAPI
```uvicorn main:app```

--------------------------------

### Dockerization
1. To create and pushing the docker container, run:
```
bash docker.sh
```

-------

### Useful Commands
- ```pip freeze > requirements.txt```
- ```.venv\Scripts\Activate.ps1```

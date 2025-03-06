# How to run

## 1. Create Virtual Environtment and Install Requirements
https://fastapi.tiangolo.com/virtual-environments/#create-a-virtual-environment

## 2. Run FastAPI
```fastapi dev main.py```

## Timescale DB
- ```docker run -d --name timescaledb -p 5432:5432 -e POSTGRES_PASSWORD=password timescale/timescaledb-ha:pg17```
- ```docker exec -it timescaledb psql -U postgres```
- ```\dx```

### Useful Commands
- ```pip freeze > requirements.txt```
- ```.venv\Scripts\Activate.ps1```
- ```alembic revision --autogenerate -m "Initial migration"``` and ```alembic upgrade head```
- Drop all migrations: ```alembic downgrade base```

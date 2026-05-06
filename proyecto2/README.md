# person-connect Lambda - Ingesta

## Descripción
Lambda de ingesta de datos de persona desde API Connect.
Se despliega como imagen Docker en ECR `ecrrdppersonlbdconnectprd01`.

## Desarrollo local
```bash
docker build -t person-connect .
docker run -p 9000:8080 person-connect
curl -X POST "http://localhost:9000/2015-03-31/functions/function/invocations" -d '{}'
```

## Tests
```bash
cd tests/
pytest -v
```

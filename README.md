# ETL Riesgo Penalizaciones

Este proyecto contiene:
- Extracción diaria de datos desde MongoDB
- Limpieza por lista negra configurable (por colección)
- Carga a S3 con partición por día
- Configuración de infraestructura (S3, CloudWatch, IAM) vía CloudFormation

## Despliegue

1. Edita `config/*_blacklist.txt` según lo que quieras excluir por colección
2. Ejecuta `template.yaml` con CloudFormation
3. Corre `extract_to_s3.py` desde EC2


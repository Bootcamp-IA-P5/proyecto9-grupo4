# Guía de Inicio Rápido: Apache Airflow

Esta guía te ayudará a configurar y ejecutar Apache Airflow para observar tu pipeline Kafka → MongoDB.

## 🚀 Inicio Rápido

### 1. Levantar Airflow con Docker

```bash
docker-compose -f docker-compose-airflow.yml up -d
```

**¡Listo!** Este comando:
- ✅ Levanta todos los contenedores de Airflow
- ✅ Crea automáticamente el usuario `admin`
- ✅ Genera una contraseña segura

### Obtener la contraseña

Espera ~1 minuto y ejecuta:

```bash
docker logs airflow-webserver 2>&1 | grep -i password
```

Verás algo como:
```
Simple auth manager | Password for user 'admin': "PASSWORD_AIRFLOW"
```

### Acceder a Airflow

**URL:** http://localhost:8080
- Usuario: `admin`
- Password: La que obtuviste arriba


---

## 🛑 Comandos Útiles

**Detener Airflow (mantiene password):**
```bash
docker-compose -f docker-compose-airflow.yml down
```

**Detener y borrar todo (regenera password):**
```bash
docker-compose -f docker-compose-airflow.yml down -v
```

**Ver logs:**
```bash
docker logs -f airflow-webserver   # Webserver
docker logs -f airflow-scheduler   # Scheduler
```

---

3. **Abrir WSL2** y seguir las instrucciones de instalación local

## 📊 Usar el DAG de Observación

### Activar el DAG

1. En la UI de Airflow, busca el DAG: `kafka_mongodb_health_monitor`
2. Activa el toggle (debe ponerse en azul/verde)
3. El DAG se ejecutará automáticamente cada 10 minutos
## 📊 Usar el DAG de Monitoreo

### 1. Activar el DAG

1. En la UI de Airflow, busca: `kafka_mongodb_health_monitor`
2. Activa el toggle (se pone azul/verde)
3. Se ejecutará automáticamente cada 10 minutos

### 2. Ejecutar Manualmente (Testing)

1. Click en el nombre del DAG
2. Click en "▶️ Trigger DAG"
3. Click en "Trigger"

### 3. Ver Logs

Click en la tarea `generate_health_summary` → "Log"

Verás algo como:
```
📊 RESUMEN DE SALUD DEL PIPELINE KAFKA → MongoDB
🚦 Estado General: HEALTHY
📦 Total documentos: 5247
🕐 Última inserción: 0:00:12
📈 Tasa inserción: 45.20 docs/min
```

---

## 🔧 Configuración Avanzada

### Cambiar frecuencia de monitoreo

Edita `airflow/dags/kafka_mongodb_observer.py`:

```python
schedule_interval='*/10 * * * *',  # Cada 10 minutos
```

Ejemplos:
- `'*/5 * * * *'` = Cada 5 minutos
- `'0 * * * *'` = Cada hora

### Cambiar base de datos MongoDB

Edita la variable de entorno en `.env`:
```bash
MONGO_ATLAS_URI=mongodb+srv://user:pass@cluster.mongodb.net/
```

---

## 🐛 Troubleshooting

**El DAG no aparece:**
- Espera 30 segundos (Airflow escanea cada 30s)
- Verifica: `docker logs airflow-scheduler`

**Error de conexión a MongoDB:**
- Verifica tu `MONGO_ATLAS_URI` en `.env`
- Asegúrate que la IP está en whitelist de MongoDB Atlas

**Ver logs completos:**
```bash
docker logs -f airflow-scheduler
```

---
```bash
airflow db reset
```

**Listar DAGs:**
```bash
airflow dags list
---

## 📚 Recursos

- [Documentación Airflow 3.0](https://airflow.apache.org/docs/apache-airflow/stable/)
- [Cron Expression Generator](https://crontab.guru/)


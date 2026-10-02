# GEMA - Gestor Empresarial de Metadatos Analíticos

Plataforma web construida sobre **Django 5.2** para la gestión de catálogos de datos, definición de un *data lake*, parametrización de consultas y ejecución de procesos **ETL dinámicos** basados en Apache Spark (PySpark). El sistema se despliega internamente.

> El código fuente vive en `c:\GEMA`, mientras que el despliegue de producción corre bajo IIS en `C:\inetpub\wwwroot\`. Varias rutas absolutas en la configuración apuntan a esa ubicación de producción.

## Tabla de contenido

- [Características](#características)
- [Arquitectura](#arquitectura)
- [Estructura del proyecto](#estructura-del-proyecto)
- [Requisitos](#requisitos)
- [Instalación](#instalación)
- [Configuración](#configuración)
- [Ejecución](#ejecución)
- [Motor ETL dinámico](#motor-etl-dinámico)
- [Rutas principales](#rutas-principales)
- [Seguridad](#seguridad)
- [Despliegue](#despliegue)
- [Notas de seguridad importantes](#notas-de-seguridad-importantes)

## Características

- **Gestión de catálogos** (`AppCatalogos`): CRUD de catálogos de datos, operaciones vía AJAX, generación/descarga de artefactos Parquet y disparo de procesos ETL.
- **Data Lake** (`AppDataLake` / vistas en `AppCatalogos`): definición de fuentes de datos, estructura de tablas, listas y columnas dinámicas.
- **Parametrización de consultas** (`AppParametrizacion`): validación y administración de consultas (Query Manager).
- **Monitoreo de archivos** (`file_monitor`): detecta automáticamente archivos nuevos (Excel, CSV, TXT), los procesa y persiste en base de datos con un dashboard web. Ver [`file_monitor/README.md`](file_monitor/README.md).
- **Autenticación LDAP / Active Directory** (`login`): inicio de sesión contra el directorio corporativo mediante `ldap3` (NTLM sobre TLS).
- **Motor ETL dinámico** (`ScriptPython/ETL_Dynamic_Engine.py`): pipeline de 10 etapas con PySpark que ingesta datos de una API, aplica metadatos y reglas, valida, transforma y exporta a Parquet.
- **Control de acceso por IP** y **expiración de sesión por inactividad** vía middleware personalizado.

## Arquitectura

```
Navegador ──> IIS (web.config / wfastcgi) ──> Django (ProyAppCatalogos)
                                                 │
          ┌──────────────────────────────────────┼───────────────────────────┐
          │                                       │                           │
   Middleware IPRestrict              Apps Django (login,            Base de datos MySQL
   + LoginRequired                    AppCatalogos, AppDataLake,     (dwhe @ 10.70.0.90)
                                      AppParametrizacion,            + bases auxiliares
                                      file_monitor)                  file_monitor
                                                 │
                                      Motor ETL dinámico (PySpark)
                                      ScriptPython/ETL_Dynamic_Engine.py
                                                 │
                                      Exportación a Parquet (C:\data\Parquet)
```

- **Proyecto Django principal:** `ProyAppCatalogos` (settings, urls, middleware, wsgi/asgi).
- **Base de datos por defecto:** MySQL (`dwhe`). Se declaran bases auxiliares `lectura_file` / `escritura_file` y un router (`db_router.FileMonitorRouter`).
- **Motor de datos:** PySpark + JDBC (MySQL connector) para el ETL, con salida en formato Parquet.

## Estructura del proyecto

```
GEMA/
├── manage.py                     # Entrypoint de administración Django
├── db_router.py                  # Router de base de datos (FileMonitor)
├── django_service.py             # Servicio de Windows para correr Django (DjangoSIMAE)
├── requirements.in / .txt        # Dependencias (gestionadas con pip-tools)
├── setup_environment.bat         # Configura entorno Python e instala dependencias
├── start_django.bat              # Arranca el servidor de desarrollo
├── ETL_Dynamic.bat               # Lanza el motor ETL manualmente
├── web.config                    # Configuración de IIS (producción)
│
├── ProyAppCatalogos/             # Proyecto Django (settings, urls, middleware, wsgi/asgi)
├── login/                        # Autenticación LDAP/AD
├── AppCatalogos/                 # Gestión de catálogos, Data Lake, ETL y Parquet
├── AppDataLake/                  # App de Data Lake
├── AppParametrizacion/           # Query Manager / parametrización de consultas
├── file_monitor/                 # Monitoreo y procesamiento automático de archivos
│
├── ScriptPython/                 # Motor ETL dinámico (PySpark) + config.json
│   ├── ETL_Dynamic_Engine.py     # Pipeline ETL de 10 etapas (0..9)
│   ├── config.json               # Configuración Spark/MySQL/API/salida
│   └── run_etl_auto.bat          # Ejecución automatizada (tarea programada)
│
├── templates/ · static/ · staticfiles/ · staticroot/   # Front-end y estáticos
├── sample_data/ · monitored_files/                      # Carpetas de datos de entrada
├── Certificado_SSL/              # Certificados del sitio
└── logs/                         # Logs de la aplicación
```

## Requisitos

- **Python 3.11**
- **MySQL** (base `dwhe` y auxiliares)
- **Java JDK 17** y **Apache Spark / PySpark** (para el motor ETL)
- **Hadoop 3.3.6** (winutils en Windows) y el **MySQL JDBC Connector 8.0.33**
- Servidor **LDAP / Active Directory** accesible (para autenticación)
- En producción: **IIS** con `wfastcgi` (archivo `web.config` incluido)

Dependencias Python principales (ver `requirements.txt`): `django==5.2.6`, `djangorestframework`, `pyarrow`, `openpyxl`, `numpy`, `watchdog`, `psutil`, `pillow`, `pycryptodome`, `grpcio`, `jinja2`, `ipython`.

> El motor ETL requiere adicionalmente `pyspark` y `requests` (ver `ScriptPython/requirements.txt`).

## Instalación

```powershell
# 1. Clonar / ubicarse en el proyecto
cd c:\GEMA

# 2. Crear y activar un entorno virtual
python -m venv venv
venv\Scripts\activate

# 3. Instalar dependencias
pip install -r requirements.txt
```

Alternativamente, en Windows puedes usar el script asistido que verifica el entorno, genera los `requirements` con `pip-tools` e instala todo:

```powershell
setup_environment.bat
```

## Configuración

La configuración vive en `ProyAppCatalogos/settings.py`. Puntos clave a revisar antes de ejecutar:

- **Base de datos** (`DATABASES`): host, nombre, usuario y contraseña de MySQL (`dwhe` por defecto).
- **LDAP** (`LDAP_SERVER`, `LDAP_SEARCH_BASE`, `LDAP_DOMAIN`, credenciales de servicio).
- **Redes/hosts permitidos** (`ALLOWED_HOSTS`, `CSRF_TRUSTED_ORIGINS`, y las subredes del middleware `IPRestrictMiddleware`).
- **ETL / Parquet** (`ETL_SCRIPTS_DIR`, `PARQUET_ROOT`).
- **Correo** (`EMAIL_*`) para notificaciones de `file_monitor`.
- **Sesión:** expiración por inactividad configurable (`SESSION_IDLE_TIMEOUT_MINUTES`, 40 min por defecto).

El motor ETL se configura por separado en `ScriptPython/config.json` (rutas de Java/Spark/Hadoop, conexión MySQL vía JDBC, URL de la API de origen, directorio de salida Parquet y parámetros de Spark).

## Ejecución

Servidor de desarrollo:

```powershell
python manage.py runserver 127.0.0.1:8000
# o
start_django.bat
```

Luego abre `http://localhost:8000/` (redirige al login).

Como servicio de Windows (producción con `pywin32`):

```powershell
python django_service.py install     # instala el servicio "DjangoSIMAE"
python django_service.py start
python django_service.py debug        # ejecución en modo debug
```

## Motor ETL dinámico

`ScriptPython/ETL_Dynamic_Engine.py` implementa un pipeline PySpark de 10 etapas (0 a 9):

0. Inicialización de configuración y sesión Spark
1. Ingesta desde API (aplana JSON anidado, sufijos dinámicos, multi-fetch)
2. Carga de metadatos de catálogo (`met_cat_dat`)
3. Carga de reglas y expresiones (`met_rgls_expr`)
4. Validación de columnas API vs. metadatos (por umbral)
5. Validación sintáctica de reglas
6. Auditoría de tipos
7. Procesamiento: fusión de BD/ETL → filtración → validación → transformación → agregación
8. Exportación de resultados/reglas/semántica a Parquet
9. Resumen final y limpieza

Ejecución manual:

```powershell
cd ScriptPython
python ETL_Dynamic_Engine.py --codcatdat COM004EX --cod_dataframe df1
```

También puede dispararse desde la aplicación web vía el endpoint `POST /api/ejecutar_etl/` y consultarse su log con `GET /api/consultar_log_etl`. Para la ejecución programada se incluyen `run_etl_auto.bat`, `register_task.ps1` y `Task_ETL_Auto.xml`.

## Rutas principales

| Ruta | Descripción |
|------|-------------|
| `/` y `/login/` | Inicio de sesión (LDAP/AD) |
| `/app/` | Aplicación principal (catálogos y data lake) |
| `/app/...` | CRUD de catálogos, data lake, listas y Query Manager (AJAX) |
| `/file-monitor/` | Dashboard de monitoreo de archivos |
| `/file-monitor/api/documents/` | API de documentos procesados |
| `POST /api/ejecutar_etl/` | Dispara el proceso ETL |
| `GET /api/consultar_log_etl` | Consulta el log del ETL |
| `/files/Parquet/<filename>/` | Descarga de artefactos Parquet |

## Seguridad

- **`IPRestrictMiddleware`**: solo permite el acceso desde IPs/subredes autorizadas (LAN, VPN y algunas IPs públicas específicas); el resto recibe una página 403 personalizada.
- **`LoginRequiredMiddleware`**: exige autenticación salvo en rutas públicas (`/login/`, `/static/`) y cierra sesión tras inactividad.
- **Autenticación LDAP** contra Active Directory con TLS.
- **Validadores de contraseña** estándar de Django y backend `django_python3_ldap`.

## Despliegue

En producción el proyecto se sirve con **IIS** mediante `wfastcgi` (ver `web.config`). Los estáticos se recopilan con:

```powershell
python manage.py collectstatic
```

y se sirven desde `staticfiles/` (`STATIC_ROOT`).

## Notas de seguridad importantes

> ⚠️ **Credenciales en el repositorio.** `ProyAppCatalogos/settings.py`, `ScriptPython/config.json` y el `README.md` de `file_monitor` contienen credenciales de base de datos, LDAP y correo en texto plano, además de una `SECRET_KEY` embebida. Antes de usar o publicar este proyecto:
>
> - Mueve todos los secretos a variables de entorno o a un gestor de secretos.
> - Rota las credenciales expuestas (DB, LDAP, correo) y regenera `SECRET_KEY`.
> - Asegúrate de que `DEBUG = False` en producción y de que `ALLOWED_HOSTS` no incluya `"*"`.
> - Revisa `.gitignore` para evitar versionar archivos sensibles.

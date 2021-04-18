# Para poder trabajar con los datos en Hive, lo primero que debemos hacer es cargar los datos.

## Subir el fichero a importar en un directorio de HDFS
```bash
hdfs dfs -mkdir /mdas
hdfs dfs -put /opt/files/dataset.csv /mdas
```

Acto seguido entramos en hive:
```bash
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000
```

## Se pide una tabla interna sin particiones ni buckets, llamada “call\_registry”
Primero creamos el database
```hive
CREATE DATABASE IF NOT EXISTS mdas;
USE mdas;
```

Creamos la tabla temporal
```hive
DROP TABLE IF EXISTS tmp_input;
CREATE TEMPORARY TABLE IF NOT EXISTS tmp_input (
	id              int,
	type            string,
	from_number     string,
	to_number       string,
	start_timestamp string,
	end_timestamp   string,
	price           string,
	call_result     string,
	event_date      string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
-- Símbolo que separa los campos
STORED AS TEXTFILE
LOCATION '/mdas/'
-- Se define la ubicación de los datos
TBLPROPERTIES ("skip.header.line.count"="1");
```

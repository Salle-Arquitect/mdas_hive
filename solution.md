# 1) Para poder trabajar con los datos en Hive, lo primero que debemos hacer es cargar los datos.

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

Transformamos la tabla
```hive
DROP TABLE IF EXISTS call_registry;
CREATE TABLE call_registry as
SELECT
	id,
	type,
	from_number,
	to_number,
	cast(regexp_replace(start_timestamp, '(\\d++)/(\\d++)/(\\d++)\\s++(\\d++):(\\d++)', '$3-$2-$1 $4:$5:00.000') as timestamp) as start_timestamp,
	cast(regexp_replace(end_timestamp,   '(\\d++)/(\\d++)/(\\d++)\\s++(\\d++):(\\d++)', '$3-$2-$1 $4:$5:00.000') as timestamp) as end_timestamp,
	cast(regexp_replace(price, ',', '.') as float) AS price,
	call_result,
	event_date
FROM tmp_input;
```

Una vez comprovado que no queda ningún NULL podemos eliminar la tabla temporal:
```hive
SELECT * FROM call_registry;
DROP TABLE tmp_input;
```

# 2) ¿Cuales son los diferentes estados en los que puede acabar una llamada (CALL)?
```hive
SELECT DISTINCT call_result FROM call_registry WHERE type = 'CALL';
+-----------------+
|   call_result   |
+-----------------+
| ANSWERED        |
| BUSY            |
| DISCONNECTED    |
| NO_RESPONSE     |
| OUT_OF_SERVICE  |
+-----------------+
```

# 3) Encuentra qué usuario se ha gastado más dinero en SMS que no han llegado a su destinatario (FAIL).
Ya que no hay ningún campo que sea usuario, interpretaremos que el usuario es el número de teléfono:
```hive
WITH SMS_FAIL as (
	SELECT type,from_number,call_result,price
	FROM call_registry
	WHERE type = 'SMS' AND call_result = 'FAIL'
)
SELECT from_number,sum(price) as total
FROM SMS_FAIL
GROUP BY from_number
SORT BY total DESC
LIMIT 1;
+--------------+--------+
| from_number  | total  |
+--------------+--------+
| 204136571    | 2.0    |
+--------------+--------+
```

# 4) ¿Cuál es la llamada que más veces se ha intentado realizar sin éxito? Se considera una llamada con éxito aquella cuyo resultado es ANSWERED
```hive
WITH NO_ANSWERED as (
	SELECT to_number,type,call_result
	FROM call_registry
	WHERE type = 'CALL' AND call_result NOT LIKE 'ANSWERED'
)
SELECT to_number,count(*) as total
FROM NO_ANSWERED
GROUP BY to_number
SORT BY total DESC
LIMIT 1;
+------------+--------+
| to_number  | total  |
+------------+--------+
| 108763829  | 10     |
+------------+--------+
```

# 5) Muestra la cantidad de llamadas del mes de diciembre de 2018, agrupadas por el resultado de la llamada (‘call\_result’).
```hive
SELECT count(id)
FROM call_registry
WHERE type = 'CALL' AND start_timestamp between '2018-12-01 00:00:00.00' and '2018-12-31 23:59:59.0';
+------+
| _c0  |
+------+
| 436  |
+------+
```

# 6) Realiza **una** consulta que muestre el top 5 de los usuarios que más han pagado en llamadas y el top 5 de los usuarios que más han pagado de SMS. Utiliza la window function rank()
```hive
SELECT from_number,type,total,rank
FROM (
	SELECT type,from_number,sum(price) as total,rank() OVER (PARTITION BY type ORDER BY sum(price) DESC) as rank
	FROM call_registry
	GROUP BY from_number,type
) as t
WHERE rank between 1 AND 5;
+--------------+-------+---------------------+-------+
| from_number  | type  |        total        | rank  |
+--------------+-------+---------------------+-------+
| 883936752    | CALL  | 11.682000160217285  | 1     |
| 253823721    | CALL  | 10.367999970912933  | 2     |
| 460946633    | CALL  | 10.277999877929688  | 3     |
| 516108166    | CALL  | 10.241999965161085  | 4     |
| 516644706    | CALL  | 10.06199997663498   | 5     |
| 470837434    | SMS   | 2.75                | 1     |
| 324415062    | SMS   | 2.75                | 1     |
| 421315475    | SMS   | 2.75                | 1     |
| 658806303    | SMS   | 2.75                | 1     |
| 140618394    | SMS   | 2.75                | 1     |
| 637639552    | SMS   | 2.75                | 1     |
| 505658865    | SMS   | 2.75                | 1     |
| 204136571    | SMS   | 2.75                | 1     |
| 400587484    | SMS   | 2.75                | 1     |
| 731290573    | SMS   | 2.75                | 1     |
| 258909358    | SMS   | 2.75                | 1     |
| 892276410    | SMS   | 2.75                | 1     |
| 812717952    | SMS   | 2.75                | 1     |
+--------------+-------+---------------------+-------+
```

# 7) Realiza **una** consulta que muestre el top 5 de los usuarios que más han pagado en llamadas y el top 5 de los usuarios que más han pagado de SMS. Sin utilizar window functions
```hive
SELECT *
FROM (
	SELECT type,from_number,SUM(price) AS price
	FROM call_registry
	WHERE type = 'CALL'
	GROUP BY from_number,type
	ORDER BY price DESC
	LIMIT 5
) AS t
UNION
SELECT *
FROM (
	SELECT type,from_number,SUM(price) AS price
	FROM call_registry
	WHERE type = 'SMS'
	GROUP BY from_number,type
	ORDER BY price DESC
	LIMIT 5
) AS d;
+-----------+------------------+---------------------+
| _u1.type  | _u1.from_number  |      _u1.price      |
+-----------+------------------+---------------------+
| CALL      | 253823721        | 10.367999970912933  |
| CALL      | 460946633        | 10.277999877929688  |
| CALL      | 516108166        | 10.241999965161085  |
| CALL      | 516644706        | 10.06199997663498   |
| CALL      | 883936752        | 11.682000160217285  |
| SMS       | 324415062        | 2.75                |
| SMS       | 421315475        | 2.75                |
| SMS       | 470837434        | 2.75                |
| SMS       | 637639552        | 2.75                |
| SMS       | 658806303        | 2.75                |
+-----------+------------------+---------------------+
```

# 8) ¿Has conseguido mostrar el mismo resultado en las preguntas 6 y 7? ¿Por qué?  Justifica la respuesta.
Todos los elementos mostrados en la pregunta 7 aparecen en el 6. El 6 da un valor de posición, haciendo que después de 8 primeros, la siguiente posición será el noveno, esto permite que haya múltiples en la misma posición a diferencia del 7, que simplemente limita la salida a una establecida, dando cierto orden de aleatoriedad entre los que tengan el mismo valor (los que entran dentro del rank).


En el mundo del futbol, se usa el método del rank, haciendo que al inicio de temporada haya muchos primeros y a medida que avanza la temporada quede solo 1.

# 9) Realiza **una** consulta que muestre los usuarios y la cantidad total de tiempo (en segundos) que han estado en una llamada durante el mes de diciembre de 2018 (hay que tener en cuenta tanto llamadas realizadas como recibidas).  Filtra los resultados para mostrar sólo aquellos usuarios que han estado más de 3 horas al teléfono.
```hive
SELECT *
FROM (
	SELECT user_phone,sum(duration) as total
	FROM (
		SELECT from_number AS user_phone, INT(end_timestamp)-INT(start_timestamp) AS duration
		FROM call_registry
		WHERE type = 'CALL'
			AND start_timestamp between '2018-12-01 00:00:00.00' and '2018-12-31 23:59:59.0'
		UNION
		SELECT to_number AS user_phone, INT(end_timestamp)-INT(start_timestamp) AS duration
		FROM call_registry
		WHERE type = 'CALL'
			AND start_timestamp between '2018-12-01 00:00:00.00' and '2018-12-31 23:59:59.0'
	) as t
	GROUP BY user_phone
) AS t
WHERE total > 3*3600;
+---------------+----------+
| t.user_phone  | t.total  |
+---------------+----------+
| 269895447     | 12360    |
| 661929270     | 12900    |
| 678267817     | 12240    |
| 750748735     | 13680    |
+---------------+----------+
```

# 10) Crea una tabla llamada 'favorite\_numbers' que guarde para cada usuario que realiza una llamada, un array con los números a los que ha hecho llamadas.
```hive
CREATE TABLE IF NOT EXISTS favorite_numbers (
	user_phone string,
	calls      ARRAY<string>
);
```

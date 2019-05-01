[![CircleCI](https://circleci.com/gh/eloirobe/mdas_hive.svg?style=svg)](https://circleci.com/gh/eloirobe/mdas_hive)

# MDAS Bases de datos no estructuradas - Hive <img src="https://hive.apache.org/images/hive_logo_medium.jpg" width="70">
Bienvenidos a la imagen de Hive para a la asignatura de Bases de datos no estructuradas del master Máster en Desarrollo y Arquitectura de Software (MDAS)

Para arrancar Hive y empezar a utilizar la imagen de docker realizar lo siguiente:

*Requisito previo tener instalado docker*

1) Clonar el repositorio
```bash
git clone https://github.com/eloirobe/mdas_hive.git
```
2) Arrancar la imagen de docker
```bash
cd mdas_hive
## En modo stdout
docker-compose up
## En modo silencioso
docker-compose up -d
```
3) Una vez haya arrancado loguearse en la console
```bash
docker-compose exec hive bash
```





## Contributors
This is a fork from [https://github.com/big-data-europe/docker-hive](https://github.com/big-data-europe/docker-hive)

* Ivan Ermilov [@earthquakesan](https://github.com/earthquakesan) (maintainer)
* Yiannis Mouchakis [@gmouchakis](https://github.com/gmouchakis)
* Ke Zhu [@shawnzhu](https://github.com/shawnzhu)

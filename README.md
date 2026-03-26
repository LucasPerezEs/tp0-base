# TP0: Docker + Comunicaciones + Concurrencia

- Alumno: Lucas Perez Esnaola
- Padrón: 107990

## Parte 1:
### Ejercicio 1

Para automatizar la creacion del docker compose se desarrollo un  script `generar-compose.sh` que llama a `compose_generator.py`, el cual se encarga de generar el archivo .yaml. Este consistira un servidor, N cantidad de clientes y una network.

Para ejecutarlo, primero se necesitan permisos de ejecucion:

`chmod +x ./generar-compose.sh`

Luego, se puede ejecutar el script de la siguiente manera:

`./generar-compose.sh <archivo de salida> <N>`

Que generara el archivo docker compose con el nombre indicado y con la cantidad de clientes indicada.
Ejemplo:

`./generar-compose.sh docker-compose-dev.yaml 5`

Luego, para ejecutar los contenedores, se ofrece un archivo Makefile que permite el siguiente comando:

`make docker-compose-up`

### Ejercicio 2
Para persistir los archivos de configuracion de cliente y servidor, y poder modificarlos sin tener que reconstruir la imagen de docker, se utilizaron `docker volumnes` con formato Bind Mount. Estos sirven para inyectar archivos que viven en el host, en los contenedores docker.

Se modifico el archivo de generacion `compose_generator.py` con las siguientes lineas:

- `/client/config.yaml:/app/config.yaml:ro`: Se le inyecta al cliente el archivo de configuracion almacenado en un docker volume, de esta forma, el archivo de configuracion no vive en la imagen, sino en el host.
- `./server/config.ini:/server/config.ini`: Lo mismo para el servidor, se le inyecta el archivo de configuracion en el host.

De esta forma, no hace falta reconstruir la imagen de docker para modificar las configuraciones del cliente o el servidor.

### Ejercicio 3
Se implemento el archivo `validar-echo-server.sh` para verificar el correcto funcionamiento del servidor, utilizando el comando `netcat`.

Para esto, el script levanta un contenedor temporal, conectado a la misma network a la que esta conectada el servidor, indicada con el flag `--network tp0_testing_net`.
Luego, envia un mensaje al servidor a traves de la red utilizando `metcat`, y espera la respuesta. Si recibe lo mismo que envio, entonces se valido correctamente el echo server.

Para ejecutarlo, primero se necesitan permisos de ejecucion:

`chmod +x ./validar-echo-server.sh`

Luego, hay que ejecutar los contenedores con:

`make docker-compose-up`

Por ultimo, se ejecuta el script con:
`./validar-echo-server.sh`

### Ejercicio 4
Para que tanto cliente como servidor manejen la señal SIGTERM y finalizen de forma ordenada, se utilizaron las librerias estandar de manejo de signals.

Para el servidor, se utilizo la libreria `signal`, que mediante la siguiente linea `signal.signal(signal.SIGTERM, self.shutdown_server)` permite detectar la señal indicada y responder con la funcion pasada como argumento. En la funcion shutdown, el servidor cierra los sockets de clientes abiertos, su propio socket y sale ordenadamente.

Para el cliente, se utilizaron las librerias `os/signal` y `syscall`. Primero se crea un canal por donde se recibira la señal, y se crea el cliente con una referencia a ese canal. En cada iteracion del cliente, se realiza una verificacion al canal para detectar si llego una señal SIGTERM, y en ese caso se corta la ejecucion.

El manejo de la señal SIGTERM es muy util al utilizar docker, ya que es la señal que se envia al realizar `docker compose down`.


## Parte 2: Repaso de Comunicaciones

### Ejercicio 5
Para este ejercicio se modifico la logica del cliente y del servidor para simular una agencia y una loteria.

Las modificaciones al cliente son:
1. Recepcion de caracteristicas de la apuesta por variables de entorno. Estas son:
 - CLI_ID
 - CLI_FIRST_NAME
 - CLI_LAST_NAME
 - CLI_DOCUMENT
 - CLI_BIRTHDATE
 - CLI_NUMBER
2. Funcion SerializeBet genera el frame a enviar al Server.
   El formato del frame es el siguiente:
   - Prefijo longitud total N (2 bytes - big endian)
   - Payload (N bytes)
     - agency id: uint8 (1 byte)
     - first_name: (1 byte len + N bytes)
     - last name: (1 byte len + N bytes)
     - document: (1 byte len + N bytes)
     - birthdate: (1 byte len + N bytes)
     - number: uint16 (2 bytes)
3. Por ultimo, el cliente queda en espera del ACK del servidor [0x01]

Las modificaciones al servidor son:
1. Al conectarse un cliente, espera a recibir los 2 bytes con el tamaño total del payload a leer
2. Luego, deserializa la apuesta del cliente con el formato mencionado
3. Persiste la apuesta en memoria con la funcion store_bets
4. Por ultimo, envia ACK al cliente [0x01]

### Ejercicio 6
En este ejercicio, los datos de apuesta del cliente ya no se leen por variables de entorno, sino que se utilizaron `docker volumes` para inyectar un set de datos con formato csv en cada cliente.
Para realizar la comunicacion por batches, se realizo lo siguiente:
El cliente envia las apuestas  con el mismo formato que en el ejercicio anterior, pero se le agrego un byte de identificador de tipo de frame:
- Frame Tipo Batch [0x01]
- Frame Tipo FIN [0x02]
Con esto, el cliente envia cada batch al servidor con tipo de frame BATCH, y cuando no tiene mas batchs por mandar, envia un ultimo frame al servidor para avisar que ya termino de mandar apuestas, con un frame tipo FIN. Esto era un requisito del ejercicio siguiente, pero se penso asi desde un principio.
Ademas, por cada batch enviado, el cliente espera recibir un ACK [0x01] del servidor si este lo leyo correctamente, o un NACK [0x02] si hubo algun error.
Ante una lectura de un NACK, el cliente implementa una serie de reintentos para reenviar el mensaje que no pudo ser procesado por el servidor.

El servidor, por su lado, primero lee un byte para identificar el tipo de mensaje.
- Si es tipo BATCH, lo deserializa con el mismo formato de antes, persiste las apuestas en memoria, y envia un ACK o un NACK en caso de error.
- Si es tipo FIN, envia un ultimo ACK al cliente, y cierra la conexion.

### Ejercicio 7
Como se menciono anteriormente, el cliente ya envia un mensaje FIN para avisar al servidor que termino de enviar batches. Luego de enviar el mensaje FIN, el cliente queda bloqueado esperando los resultados del sorteo.

El servidor ahora recibe por variable de entorno la cantidad de clientes esperados, y lleva un contador de cuantos clientes ya enviaron el frame FIN.
Cuando todos los clientes enviaron FIN, procesa todas las apuestas persistidas y obtiene todos los ganadores del sorteo segun cliente (agencia). 

El servidor genera el siguiente frame que se envia por socket a cada cliente:
- 1 byte Tipo mensaje WINNERS [0x04]
- 2 bytes longitud todal del payload (big endian)
- Por cada ganador:
 - 2 bytes longitud total del documento (big endian)
 - N bytes payload documento

Luego, espera a recibir los ACKs de los clientes para cerrar la conexion. En caso de recibir algun NACK, implementa una serie de reintentos. 

## Parte 3: Repaso de Concurrencia
### Ejercicio 8
Para implementar la concurrencia en el servidor, se utilizo la libreria `threading` de Python. La implementacion de threads en Python esta limitada por el GIL (GlobalInterpreterLock), que impide tener una concurrencia en el CPU. Sin embargo, el mayor trabajo del servidor son operaciones de red y I/O, y este tipo de operaciones liberan el GIL. Por lo que se decidio, tambien por una cuestion de simplicidad, utilizar la libreria `threading` aun sabiendo que para tareas que consuman CPU no existira una concurrencia real.

El servidor:
- Lanza un thread por cada cliente que se conecta y lo guarda en self.threads para luego joinearlos. Cada thread se encarga de la comunicacion con el cliente conectado.
- Para acceder a variables compartidas como self.client_sockets, o operaciones de escritura/lectura en archivo, se utilizo un lock.
- Para sincronizar el sorteo se implemento una Barrier. Cuando llega el mensaje FIN de un cliente, el handler se queda bloqueado en la barrera hasta que todos los clientes hayan llegado a ese punto. Cuando todos llegaron, significa que todas las apuestas fueron persistidas, y recien en ese momento , cada thread calcula los ganadores de la agencia con la que se esta comunicando, y envia los ganadores.



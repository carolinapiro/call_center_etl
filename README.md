# Contexto y Objetivos

Este proyecto contiene el código para popular un DataMart con datos de un CallCenter.

## Contexto del negocio

La empresa coloca micro préstamos a distintos clientes: pequeños tenderos y personas con limitados accesos a créditos, para que puedan financiar mejor sus compras y tener mayor liquidez. 

Utilizando el Call Center, se realiza el seguimiento y acercamiento con estos clientes. 

Mediante llamadas, se los incentiva a usar los canales digitales. 

# Estructura del Proceso ETL

El repositorio del proyecto incluye:
- una notebook con análisis exploratorio inicial de los datos.
- un archivo principal, ejecutable desde consola, y
- clases especializadas para: la conexión al DataMart y la lógica de carga de cada una de sus tablas.

## Lógica de carga 

En el archivo principal:
- Se consulta la tabla de control dm_processes para obtener los períodos de tiempo a cargar en cada tabla fact (en este caso Fact Llamadas)
- Para cada período a cargar:
    - Se actualizan las dimensiones relacionadas
    - Se actualiza la fact
    - Se actualiza el estado de los períodos en la tabla de control

Lógica de carga para Dimensiones:
- Se trunca la tabla stg o intermedia.
- Se extraen todos los datos de la fuente.
- Se limpian y estandarizan los datos fuente y 
- se los carga en la tabla stg.
- En caso de que la dimensión tenga claves foráneas a otras dimensiones:
    - Se validan los id naturales de las dimensiones foráneas
    - Se mapean los ids naturales de las dimensiones a las key surrogadas utilizadas en las tablas Dim
- Se cargan los datos de la tabla stg a la Dim, usando la lógica de Slowly Changing Dimension, con atributos cambiantes (no hay atributos históricos o fijos en este caso)

Lógica de carga para Facts:
- Se trunca la tabla stg o intermedia.
- Se extraen los datos de la fuente, filtrando para el período a cargar.
- Se limpian y estandarizan los datos fuente y 
- se los carga en la tabla stg.
- Para las claves foráneas a dimensiones:
    - Se validan los id naturales de las dimensiones foráneas
    - Se mapean los ids naturales de las dimensiones a las key surrogadas utilizadas en las tablas Dim
- Se cargan los datos de la tabla stg a la Fact, primero eliminando los datos existentes para el período.

Adicionalmente, al final del proceso se calculan métricas necesarias para el análisis.

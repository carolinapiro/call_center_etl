from tables.Clientes import Clientes
from tables.CanalesDigitales import CanalesDigitales
from tables.Agentes import Agentes
from tables.Logins import Logins
from tables.Llamadas import Llamadas
from tables.DM_Processes import DM_Processes


# Consultar procesos a cargar -----------------------------------------------------

procesos = DM_Processes()
# procesos.insertar_proceso_diario()
procesos_pendientes = procesos.buscar_procesos_pendientes()

if procesos_pendientes.shape[0] != 0:

    # Carga Dimensiones relacionadas -------------------------------
    
    print("Loading dims")

    # Clientes
    clientes = Clientes()
    clientes.procesar_carga_dim()

    # Canales Digitales
    canales = CanalesDigitales()
    canales.procesar_carga_dim()

    # Agentes
    agentes = Agentes()
    agentes.procesar_carga_dim()

    # Logins
    logins = Logins()
    logins.procesar_carga_dim_fks()


    for index, proceso in procesos_pendientes.iterrows():

        if proceso.fact_table == "fact_llamadas":

            # Actualiza estado proceso -------------------------------------

            procesos.actualizar_estado_proceso_corriendo(proceso.id_process)

            print("Ongoing process updated")


            # Carga Fact ---------------------------------------------------
            
            print("Loading fact")

            # Llamadas
            llamadas = Llamadas(start_date=proceso.start_date,end_date=proceso.end_date)
            llamadas.procesar_carga_fact()


            # Actualiza estado proceso -------------------------------------

            procesos.actualizar_estado_proceso_exitoso(proceso.id_process)

            print("Successful process updated")
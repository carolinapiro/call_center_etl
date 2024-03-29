
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

for index, proceso in procesos_pendientes.iterrows():

    if proceso.fact_table == "fact_llamadas":

        # Actualiza estado proceso -------------------------------------

        procesos.actualizar_estado_proceso_corriendo(proceso.id_process)


        # Carga Dimensiones relacionadas -------------------------------

        # Clientes
        clientes = Clientes()
        clientes.procesar_carga_clientes()

        # Canales Digitales
        canales = CanalesDigitales()
        canales.procesar_carga_canales()

        # Agentes
        agentes = Agentes()
        agentes.procesar_carga_agentes()

        # Logins
        logins = Logins()
        logins.procesar_carga_logins()


        # Carga Fact ---------------------------------------------------

        # Llamadas
        llamadas = Llamadas(start_date=proceso.start_date,end_date=proceso.end_date)
        llamadas.procesar_carga_llamadas()


        # Actualiza estado proceso -------------------------------------

        procesos.actualizar_estado_proceso_exitoso(proceso.id_process)
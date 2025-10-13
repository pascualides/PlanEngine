from time import sleep
from DB.database_cnx import get_registros_by_id, get_registro_by_params, get_reglas, get_ejecutor_plan, insert_table, update_registro_estado
from etlrules import Plan, RuleData
from MotorReglas.ruleEngine import RuleEngine
from MotorReglas.reglas import RULES_MAP
import pandas as pd
import logging
from datetime import datetime
from Utils.logger import Logger

PROCESANDO = False

class GestorEjecucion:
    def __init__(
        self,
        plan: int,
        input_data: pd.DataFrame = None,
        input_name: str = 'input',
        named_inputs: dict = None,        
    ):
        self.plan_id = plan
        self.input_data = input_data
        try:
            self.plan = get_plan_df(self.plan_id)
            if not named_inputs:
                self.data = RuleData(named_inputs={input_name: self.input_data})
            else:
                self.data = RuleData(named_inputs=named_inputs)
            self.rule_engine = RuleEngine(self.plan)
        except Exception as e:
            error = f'Error al cargar el plan {self.plan_id}:\n {type(e).__name__}: {e}'
            raise Exception(error) from None
            

    def ejecutar(self):
        try:
            self.rule_engine.run(self.data)
        except Exception as e:
            error = f" Error al ejecutar el plan {self.plan_id}: \n{type(e).__name__}: {e}"
            raise Exception(error) from None


    def get_salidas(self):
        salidas = get_registro_by_params('Salidas', {'sal_plan_id': self.plan_id})
        res = {}
        for nombre in salidas['sal_nombre_df'].to_list():
            res[nombre] = self.data.get_named_output(nombre) 
        return res
    

    def set_plan(self, plan_id):
        try:
            self.plan_id = plan_id
            self.plan = get_plan_df(self.plan_id)
            self.rule_engine = RuleEngine(self.plan)
        except Exception as e:
            error = f'Error al cargar el plan {self.plan_id}:\n {type(e).__name__}: {e}'
            raise Exception(error) from None
        
        
    def get_data(self):
        return self.data


def get_regla_parametrizada(parametros_df, reglas_secundarias=None):
    params_dict = {}
    for _, par_row in parametros_df.iterrows():
                if par_row['tip_par_tipo'] == 'lista':
                    params_dict[par_row['tip_par_nombre_int']] = parametros_df.loc[parametros_df['par_id'] == par_row['par_id']]['val_valor'].to_list()
                elif par_row['tip_par_tipo'] == 'unico':
                    params_dict[par_row['tip_par_nombre_int']] = parametros_df.loc[parametros_df['par_id'] == par_row['par_id']]['val_valor'].iloc[0]
                elif par_row['tip_par_tipo'] == 'boolean':
                    params_dict[par_row['tip_par_nombre_int']] = parametros_df.loc[parametros_df['par_id'] == par_row['par_id']]['val_valor'].iloc[0] == 'True'
                elif par_row['tip_par_tipo'] == 'int':
                    params_dict[par_row['tip_par_nombre_int']] = int(parametros_df.loc[parametros_df['par_id'] == par_row['par_id']]['val_valor'].iloc[0])
    if reglas_secundarias:
        params_dict['rules'] = reglas_secundarias
    params_dict['name'] = parametros_df['reg_id'].iloc[0]
    regla_obj = RULES_MAP.get(parametros_df['tip_reg_nombre_int'].iloc[0])
    return regla_obj(**params_dict)


def get_reglas_secundarias(reglas_df):
    list_reglas = []
    try:
        for row in reglas_df['reg_id'].unique():
            parametros_df = reglas_df.loc[reglas_df['reg_id'] == row]
            regla = get_regla_parametrizada(parametros_df)
            list_reglas.append(regla)
    except Exception as e:
        error = f'Error al cargar la regla secundaria {row}:\n {type(e).__name__}: {e}'
        raise Exception(error) from None
    return list_reglas


def get_plan_df(id_plan):
    plan = Plan()
    plan_df = get_registros_by_id('Plan', 'plan_id', [id_plan])
    if len(plan_df) != 1:
        raise Exception(f'No se encontró el plan con id {id_plan}')
        
    reglas_df = get_reglas(id_plan)
    if len(reglas_df) == 0:
        raise Exception(f'El plan con id {id_plan} no tiene reglas asociadas')

    #Dividir en reglas principlales y secundarias
    ids_regla_bloque = []
    if 'reg_bloque_regla_id' in reglas_df.columns:
        reglas_sec_df = reglas_df[~reglas_df['reg_bloque_regla_id'].isna()].sort_values(by='reg_orden')
        reglas_df = reglas_df[reglas_df['reg_bloque_regla_id'].isna()].sort_values(by='reg_orden')

        ids_regla_bloque = reglas_sec_df['reg_bloque_regla_id'].unique().tolist()

    for id_regla in reglas_df['reg_id'].unique(): #For por regla principal
        try:
            list_reglas_sec = None
            #ver si el id esta en los unique
            if id_regla in ids_regla_bloque:
                #si es asi crear primero esas reglas, almacenarlas
                list_reglas_sec = get_reglas_secundarias(reglas_sec_df[reglas_sec_df['reg_bloque_regla_id'] == id_regla])

            parametros_df = reglas_df.loc[reglas_df['reg_id'] == id_regla]
            #Si list_reglas_sec tiene valores, se agrega a los parametros como "rules" para que se ejecute en bloque de reglas
            regla = get_regla_parametrizada(parametros_df, list_reglas_sec)

            plan.add_rule(regla)
        except Exception as e:
            error = f'Error al cargar la regla {id_regla} del plan {id_plan}:\n {type(e).__name__}: {e}'
            Logger.error(error)
            raise Exception(error) from None

    return plan


def set_estado_ejecutor(id_ejecutor, id_estado):
    insert_table("Estados", dict={'est_id_tipo': id_estado, 'est_id_ejec': id_ejecutor})
    update_registro_estado("Ejecutor", {'ejec_id': id_ejecutor}, {'ejec_id_tes': id_estado})


def iniciar_ejecucion(id_ejecutor):
    #Buscar ejecutor con estado y plan
    ejecutor = get_ejecutor_plan(id_ejecutor)
    if len(ejecutor) == 0:
        msj_error = f'No se encontró el ejecutor con id {id_ejecutor}'
        Logger.error(msj_error)
        raise Exception(msj_error)

    #Validar estado
    if ejecutor['tes_nombre'].iloc[0] != 'PENDIENTE':
        msj_error= f'El ejecutor con id {id_ejecutor} no está en estado PENDIENTE'
        Logger.error(msj_error)
        raise Exception(msj_error)
    
    #Poner a procesando 
    set_estado_ejecutor(id_ejecutor, 2)
    
    #Buscar entradas
    entradas = get_registro_by_params('Entradas', {'ent_id_ejecutor': id_ejecutor})
    #Tirar error si no hay entradas
    if len(entradas) == 0:
        set_estado_ejecutor(id_ejecutor, 4)
        msj_error = f'No se encontraron entradas para el ejecutor con id {id_ejecutor}'
        Logger.error(msj_error)
        raise Exception(msj_error)
    input = {}
    for _, row in entradas.iterrows():
        try:
            input[row['ent_nombre']] = pd.read_csv(row['ent_ubicacion'])
        except Exception as e:
            set_estado_ejecutor(id_ejecutor, 4)
            msj_error = f'Error al leer la entrada {row["ent_nombre"]} desde {row["ent_ubicacion"]}:\n {type(e).__name__}: {e}'
            Logger.error(msj_error)
            raise Exception(msj_error) from None

    #Buscar salidas
    salidas = get_registro_by_params('Salidas', {'sal_ejec_id': id_ejecutor})
        #Tirar error si no hay salidas
    if len(salidas) == 0:
        set_estado_ejecutor(id_ejecutor, 4)
        msj_error = f'No se encontraron salidas para el ejecutor con id {id_ejecutor}'
        Logger.error(msj_error)
        raise Exception(msj_error)
        
    #Armar rule data
    data = RuleData(named_inputs=input)
   
    #Buscar plan
    try:
        plan = get_plan_df(int(ejecutor['plan_id'].iloc[0]))
    except Exception as e:
        set_estado_ejecutor(id_ejecutor, 4)
        raise e
    
    #Crear rule engine con el plan 
    rule_engine = RuleEngine(plan)

    #Ejecutar rule engine
    try:
        rule_engine.run(data)
    except Exception as e:
        set_estado_ejecutor(id_ejecutor, 4)
        error = f" Error al ejecutar el plan {ejecutor['plan_id'].iloc[0]}: \n{type(e).__name__}: {e}"
        raise Exception(error) from None

    #Guardar salidas
    for _, row in salidas.iterrows():
        result = data.get_named_output(row['sal_nombre_df'])
        result_name = row['sal_nombre']  
        result.to_csv(f"Archivos/Salida/{result_name}.csv",sep=';', index=False)
    #Estado finalizado
    set_estado_ejecutor(id_ejecutor, 3)


def ejecucion_automatica():
    global PROCESANDO
    #Buscar ejecutores pendientes
    if PROCESANDO:
       return
    
    PROCESANDO = True
     
    ejecutores = get_registro_by_params('Ejecutor', {'ejec_id_tes': 1})
    if len(ejecutores) > 0:
        print('Ejecutando')
        for _, row in ejecutores.iterrows():
            Logger.set_ejecutor(row['ejec_id'])
            try:
                iniciar_ejecucion(int(row['ejec_id']))
            except Exception as e:
                logging.error(f'Error en la ejecucion {row["ejec_id"]}:\n {type(e).__name__}: {e}')
        print("Guardando logs")
        id_ejec = Logger.get_ejecutor()
        logs = Logger.get_logs()
        guardar_logs(id_ejec, logs)
        Logger.clear()

    PROCESANDO = False
    

def guardar_logs(id_ejec:int, logs:list ):
    if len(logs) > 0:
        logs_db = [
        {
            'log_tipo': log.tipo,
            'log_msg': log.mensaje,
            'log_fecha_inicio': log.inicio,
            'log_fecha_fin': log.fin,
            'log_duracion': log.duracion,
            'log_id_regla': log.regla,
            'log_id_ejecutor': id_ejec
        }
        for log in logs
        ]
        insert_table('Logs', list_dict=logs_db)
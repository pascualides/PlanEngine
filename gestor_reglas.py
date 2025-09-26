from DB.database_cnx import get_registros_by_id, get_registro_by_params, get_reglas, get_ejecutor_plan, insert_table, update_registro_estado
#from etlrules.backends.pandas import ProjectRule, RenameRule, SortRule, FilterRule, AddNewColumnRule, VConcatRule
from etlrules import Plan, RuleData, RuleEngine
from reglas import RULES_MAP
import pandas as pd
import logging
from datetime import datetime

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


def get_plan_df(id_plan):
    plan = Plan()
    plan_df = get_registros_by_id('Plan', 'plan_id', [id_plan])
    if len(plan_df) != 1:
        raise Exception(f'No se encontró el plan con id {id_plan}')
        
    reglas_df = get_reglas(id_plan)
    if len(reglas_df) == 0:
        raise Exception(f'El plan con id {id_plan} no tiene reglas asociadas')
    reglas_df = reglas_df.sort_values(by='reg_orden')

    for row in reglas_df['reg_id'].unique():
        try:
            params_dict = {}
            parametros_df = reglas_df.loc[reglas_df['reg_id'] == row]
            for _, par_row in parametros_df.iterrows():
                if par_row['tip_par_tipo'] == 'lista':
                    params_dict[par_row['tip_par_nombre_int']] = parametros_df.loc[parametros_df['par_id'] == par_row['par_id']]['val_valor'].to_list()
                elif par_row['tip_par_tipo'] == 'unico':
                    params_dict[par_row['tip_par_nombre_int']] = parametros_df.loc[parametros_df['par_id'] == par_row['par_id']]['val_valor'].iloc[0]
                elif par_row['tip_par_tipo'] == 'boolean':
                    params_dict[par_row['tip_par_nombre_int']] = parametros_df.loc[parametros_df['par_id'] == par_row['par_id']]['val_valor'].iloc[0] == 'True'
                elif par_row['tip_par_tipo'] == 'int':
                    params_dict[par_row['tip_par_nombre_int']] = int(parametros_df.loc[parametros_df['par_id'] == par_row['par_id']]['val_valor'].iloc[0])

            regla_obj = RULES_MAP.get(parametros_df['tip_reg_nombre_int'].iloc[0])

            plan.add_rule(regla_obj(**params_dict))
        except Exception as e:
            error = f'Error al cargar la regla {row} del plan {id_plan}:\n {type(e).__name__}: {e}'
            raise Exception(error) from None

    return plan

def set_estado_ejecutor(id_ejecutor, id_estado):
    insert_table("Estados", dict={'est_id_tipo': id_estado, 'est_id_ejec': id_ejecutor})
    update_registro_estado("Ejecutor", {'ejec_id': id_ejecutor}, {'ejec_id_tes': id_estado})

def iniciar_ejecucion(id_ejecutor):
    #Buscar ejecutor con estado y plan
    ejecutor = get_ejecutor_plan(id_ejecutor)
    if len(ejecutor) == 0:
        raise Exception(f'No se encontró el ejecutor con id {id_ejecutor}')

    #Validar estado
    if ejecutor['tes_nombre'].iloc[0] != 'PENDIENTE':
        raise Exception(f'El ejecutor con id {id_ejecutor} no está en estado PENDIENTE')
    
    #Poner a procesando 
    set_estado_ejecutor(id_ejecutor, 2)
    
    #Buscar entradas
    entradas = get_registro_by_params('Entradas', {'ent_id_ejecutor': id_ejecutor})
    #Tirar error si no hay entradas
    if len(entradas) == 0:
        set_estado_ejecutor(id_ejecutor, 4)
        raise Exception(f'No se encontraron entradas para el ejecutor con id {id_ejecutor}')
    input = {}
    for _, row in entradas.iterrows():
        try:
            input[row['ent_nombre']] = pd.read_csv(row['ent_ubicacion'])
        except Exception as e:
            set_estado_ejecutor(id_ejecutor, 4)
            raise Exception(f'Error al leer la entrada {row["ent_nombre"]} desde {row["ent_ubicacion"]}:\n {type(e).__name__}: {e}') from None

    #Buscar salidas
    salidas = get_registro_by_params('SalidasEJE', {'sal_ejec_id': id_ejecutor})
        #Tirar error si no hay salidas
    if len(salidas) == 0:
        set_estado_ejecutor(id_ejecutor, 4)
        raise Exception(f'No se encontraron salidas para el ejecutor con id {id_ejecutor}')
        
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
    
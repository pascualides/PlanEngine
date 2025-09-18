from database_cnx import get_registros_by_id, get_registro_by_params, get_reglas
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
        input_data: pd.DataFrame,
        input_name: str = 'input'        
    ):
        self.plan_id = plan
        self.input_data = input_data
        try:
            self.plan = get_plan_df(self.plan_id)
            self.data = RuleData(named_inputs={input_name: self.input_data})
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
        return
        
    reglas_df = get_reglas(id_plan)
    if len(reglas_df) == 0:
        return
    reglas_df.sort_values(by='reg_orden')

    for row in reglas_df['reg_id'].unique():
        params_dict = {}
        parametros_df = reglas_df.loc[reglas_df['reg_id'] == row]
        for _, par_row in parametros_df.iterrows():
            if par_row['tip_par_tipo'] == 'lista':
                params_dict[par_row['tip_par_nombre_int']] = parametros_df.loc[parametros_df['par_id'] == par_row['par_id']]['val_valor'].to_list()
            elif par_row['tip_par_tipo'] == 'unico':
                params_dict[par_row['tip_par_nombre_int']] = parametros_df.loc[parametros_df['par_id'] == par_row['par_id']]['val_valor'].iloc[0]
            elif par_row['tip_par_tipo'] == 'boolean':
                params_dict[par_row['tip_par_nombre_int']] = parametros_df.loc[parametros_df['par_id'] == par_row['par_id']]['val_valor'].iloc[0] == 'True'

        regla_obj = RULES_MAP.get(parametros_df['tip_reg_nombre_int'].iloc[0])

        plan.add_rule(regla_obj(**params_dict))

    return plan
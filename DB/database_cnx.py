from sqlalchemy import create_engine, MetaData, Table, select, and_, or_, bindparam
import pandas as pd
import numpy as np
import logging
import time
import os

logger = logging.getLogger(__name__)


host = "localhost"
port = 5432
usr = "postgres"
pwd = 123456
db = "RulesEngine"

# CREATE DATABASE CONNECTION
# engine = create_engine(f'oracle+cx_oracle://{usr}:{pwd}@{host}:{port}/{db}', echo=False, max_identifier_length=128)
engine = create_engine(f'postgresql+psycopg2://{usr}:{pwd}@{host}:{port}/{db}')

get_registros_by_id, get_registro_by_params, get_reglas, get_ejecutor_plan, insert_table, update_registro_estado

def get_registro_by_params(table_name, where_dict):
    try:
        metadata_obj = MetaData()
        table = Table(table_name, metadata_obj, autoload_with=engine)

        conditions = []
        for column, value in where_dict.items():
            conditions.append(table.c[column] == value)

        with engine.connect() as conn:
            result = conn.execute(table.select().where(and_(*conditions)))
            df = pd.DataFrame(result.fetchall())
            if not df.empty:
                df.columns = result.keys()

        df = df.dropna(axis='columns', how='all')

        return df
    except Exception as e:
        logging.error('%s - %s', 'get_registro_by_params', str(e))
        raise Exception()


def get_registros_by_id(table_name, table_col_id, id_list):
    try:
        logging.info(f'BUSCANDO {table_name} POR ID')

        metadata_obj = MetaData()
        table = Table(table_name, metadata_obj, autoload_with=engine)

        idx_max = len(id_list)
        idx = 0

        df = pd.DataFrame()
        with engine.connect() as conn:
            while idx < idx_max:
                if idx+1000 < idx_max:
                    result = conn.execute(table.select().where(table.c[table_col_id].in_(id_list[idx:idx+1000])))

                else:
                    result = conn.execute(table.select().where(table.c[table_col_id].in_(id_list[idx:])))

                aux_df = pd.DataFrame(result.fetchall())
                if not aux_df.empty:
                    aux_df.columns = result.keys()
                df = pd.concat([df, aux_df])
                idx = idx + 1000

        if len(df) == 0:
            return df

        df = df.dropna(axis='columns', how='all')

        return df
    except Exception as e:
        logging.error('%s - %s', 'get_registros_by_id', str(e))
        raise Exception()


def update_registro_estado(table_name, where_dict, set_dict):
    try:
        metadata_obj = MetaData()
        table = Table(table_name, metadata_obj, autoload_with=engine)

        conditions = []
        for column, value in where_dict.items():
            conditions.append(table.c[column] == value)

        with engine.connect() as conn:
            conn.execute(table.update().where(and_(*conditions)).values(set_dict))
            conn.commit()

    except Exception as e:
        logging.error('%s - %s', 'update_registro_estado', str(e))
        raise Exception()


def get_reglas(id_plan):
    try:
        metadata_obj = MetaData()
        reglas = Table('Reglas', metadata_obj, autoload_with=engine)
        tip_reglas = Table('Tipo_Regla', metadata_obj, autoload_with=engine)
        parametros = Table('Parametros', metadata_obj, autoload_with=engine)
        tip_parametros = Table('Tipo_Parametros', metadata_obj, autoload_with=engine)
        valores = Table('Valores', metadata_obj, autoload_with=engine)
        df = None

        with engine.connect() as conn:
            result = conn.execute(select('*').select_from(reglas.join(tip_reglas, tip_reglas.c['tip_reg_id'] == reglas.c['reg_tipo_regla'])
                                  .join(parametros, parametros.c['par_reg_id'] == reglas.c['reg_id'])
                                  .join(tip_parametros, tip_parametros.c['tip_par_id'] == parametros.c['par_tipo_par'])
                                  .join(valores, valores.c['val_param_id'] == parametros.c['par_id'])
                                  ).where(reglas.c['reg_plan_id'] == id_plan))
            df = pd.DataFrame(result.fetchall())
            if len(df) == 0:
                return df
            df.columns = result.keys()

        df = df.dropna(axis='columns', how='all')

        return df
    except Exception as e:
        logging.error('%s - %s', 'get_pagos_incluidos', str(e))
        raise Exception()
    
def get_ejecutor_plan(id_ejecutor):
    try:
        metadata_obj = MetaData()
        ejecutor = Table('Ejecutor', metadata_obj, autoload_with=engine)
        tip_estado = Table('Tipos_Estado', metadata_obj, autoload_with=engine)
        plan = Table('Plan', metadata_obj, autoload_with=engine)

        df = None

        with engine.connect() as conn:
            result = conn.execute(select('*').select_from(ejecutor.join(tip_estado, tip_estado.c['tes_id'] == ejecutor.c['ejec_id_tes'])
                                  .join(plan, plan.c['plan_id'] == ejecutor.c['ejec_id_plan'])
                                  ).where(ejecutor.c['ejec_id'] == id_ejecutor))
            df = pd.DataFrame(result.fetchall())
            if len(df) == 0:
                return df
            df.columns = result.keys()

        df = df.dropna(axis='columns', how='all')

        return df
    except Exception as e:
        logging.error('%s - %s', 'get_ejecutor_plan', str(e))
        raise Exception()
    
def insert_table(table_name, df=None, dict=None, list_dict=None):
    try:
        metadata_obj = MetaData()
        table = Table(table_name, metadata_obj, autoload_with=engine)

        if df:
            df = df.replace({np.nan: None, 'nan': None})
            insert_dict = df.to_dict('records')
        elif list_dict:
            insert_dict = list_dict
        else:
            insert_dict = []
            insert_dict.append(dict)

        with engine.connect() as conn:
            conn.execute(table.insert(), insert_dict)
            conn.commit()

    except Exception as e:
        logging.error('%s - %s', 'insert_table', str(e))
        raise Exception()
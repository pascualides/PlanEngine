from sqlalchemy import create_engine, MetaData, Table, select, and_, or_, bindparam
import pandas as pd
import numpy as np
import logging
import time
import os

logger = logging.getLogger(__name__)

# # AMBIENTE
# host = os.environ["NEWCON_DB_HOST"]
# port = os.environ["NEWCON_DB_PORT"]
# usr = os.environ["NEWCON_DB_USER"]
# pwd = os.environ["NEWCON_DB_PASS"]
# db = os.environ["NEWCON_DB_NAME"]

# OOOORACLE
# host = "conciliador-db"
# host = "localhost"
# port = 1521
# usr = "newdb"
# pwd = "newdb"
# db = "xe"

# host = "rentasdb-desa.channohdywqh.us-west-2.rds.amazonaws.com"
# port = 1521
# usr = "DES_CONCILIADOR"
# pwd = "c0nc1l14d0r"
# db = "RENTASDB"

host = "localhost"
port = 5432
usr = "postgres"
pwd = 123456
db = "RulesEngine"

# CREATE DATABASE CONNECTION
# engine = create_engine(f'oracle+cx_oracle://{usr}:{pwd}@{host}:{port}/{db}', echo=False, max_identifier_length=128)
engine = create_engine(f'postgresql+psycopg2://{usr}:{pwd}@{host}:{port}/{db}')


def get_comprobantes(entidad, fecha_min, fecha_max):
    try:
        logging.info('BUSCANDO COMPROBANTES')

        metadata_obj = MetaData()
        comprobantes = Table('comprobantes', metadata_obj, autoload_with=engine)
        df = None

        with engine.connect() as conn:
            result = conn.execute(comprobantes.select().where(and_(or_(comprobantes.c['cmp_estado'] == 9,
                                                                       comprobantes.c['cmp_estado'] == 11),
                                                                   comprobantes.c['cmp_entidad'] == entidad,
                                                                   comprobantes.c['cmp_fecha_alta'] >= fecha_min,
                                                                   comprobantes.c['cmp_fecha_alta'] <= fecha_max,
                                                                   comprobantes.c['cmp_operacion'] == None)))
            df = pd.DataFrame(result.fetchall())
            if len(df) == 0:
                return df
            df.columns = result.keys()

        df = df.dropna(axis='columns', how='all')

        return df
    except Exception as e:
        logging.error('%s - %s', 'get_comprobantes', str(e))
        raise Exception()


def get_comprobantes_por_comercio(nro_comercio, fecha_min, fecha_max):
    try:
        logging.info('BUSCANDO COMPROBANTES')

        metadata_obj = MetaData()
        comprobantes = Table('comprobantes', metadata_obj, autoload_with=engine)
        df = None

        with engine.connect() as conn:
            result = conn.execute(comprobantes.select().where(and_(or_(comprobantes.c['cmp_estado'] == 9,
                                                                       comprobantes.c['cmp_estado'] == 11),
                                                                   comprobantes.c['cmp_nro_comercio'] == nro_comercio,
                                                                   comprobantes.c['cmp_fecha_alta'] >= fecha_min,
                                                                   comprobantes.c['cmp_fecha_alta'] <= fecha_max,
                                                                   comprobantes.c['cmp_operacion'] == None)))
            df = pd.DataFrame(result.fetchall())
            if len(df) == 0:
                return df
            df.columns = result.keys()

        df = df.dropna(axis='columns', how='all')

        return df
    except Exception as e:
        logging.error('%s - %s', 'get_comprobantes_por_comercio', str(e))
        raise Exception()


def get_pagos_rendicion(id_ente, fecha_inf, fecha_clearing):
    try:
        logging.info('BUSCANDO PAGOS INCLUIDOS')

        metadata_obj = MetaData()
        pagos = Table('registros', metadata_obj, autoload_with=engine)
        ren_det = Table('rendiciones_det', metadata_obj, autoload_with=engine)
        df = None

        with engine.connect() as conn:
            result = conn.execute(select(['*']).select_from(
                pagos.join(ren_det, pagos.c['reg_id'] == ren_det.c['ren_det_id_registro'], isouter=True)).where(
                (pagos.c['reg_estado'] == 3) &
                (pagos.c['reg_entidad'] == id_ente) &
                (pagos.c['reg_fecha_rendicion_inf'] == fecha_inf) &
                (pagos.c['reg_fecha_clearing'] == fecha_clearing) &
                (pagos.c['reg_tipo'] == 'P') &
                (ren_det.c['ren_det_id'] == None)))

            df = pd.DataFrame(result.fetchall())
            if len(df) == 0:
                return df
            df.columns = result.keys()

        df = df.dropna(axis='columns', how='all')

        return df
    except Exception as e:
        logging.error('%s - %s', 'get_pagos_incluidos', str(e))
        raise Exception()


def get_pagos_incluidos(id_ente, fecha_inf, fecha_clearing):
    try:
        logging.info('BUSCANDO PAGOS INCLUIDOS')

        metadata_obj = MetaData()
        pagos = Table('registros', metadata_obj, autoload_with=engine)
        ren_det = Table('rendiciones_det', metadata_obj, autoload_with=engine)
        df = None

        with engine.connect() as conn:
            result = conn.execute(select(['*']).select_from(
                pagos.join(ren_det, pagos.c['reg_id'] == ren_det.c['ren_det_id_registro'], isouter=True)).where(
                (pagos.c['reg_entidad'] == id_ente) &
                (or_(pagos.c['reg_estado'] == 3, pagos.c['reg_estado'] == 35)) &
                (pagos.c['reg_fecha_rendicion_inf'] < fecha_inf) &
                (pagos.c['reg_fecha_clearing'] == fecha_clearing) &
                (pagos.c['reg_tipo'] == 'P') &
                (ren_det.c['ren_det_id'] == None)))
            df = pd.DataFrame(result.fetchall())
            if len(df) == 0:
                return df
            df.columns = result.keys()

        df = df.dropna(axis='columns', how='all')

        return df
    except Exception as e:
        logging.error('%s - %s', 'get_pagos_incluidos', str(e))
        raise Exception()


def update_fileupload(nombre_archivo, estado, new_estado, resultado):
    try:
        metadata_obj = MetaData()
        table = Table('fileupload', metadata_obj, autoload_with=engine)
        stmt = table.update().where(
            (table.c['fup_nombre_archivo'] == nombre_archivo) & (table.c['fup_estado'] == estado)).values(
            fup_estado=new_estado, fup_resultado=resultado)

        with engine.connect() as conn:
            conn.execute(stmt)

    except Exception as e:
        logging.error('%s - %s', 'update_fileupload', str(e))
        raise Exception()


def get_pagos_por_comision(ente, fecha_desde, fecha_hasta, considerar_metodo, metodo):
    try:
        metadata_obj = MetaData()
        table = Table('registros', metadata_obj, autoload_with=engine)

        df = None

        with engine.connect() as conn:
            if considerar_metodo:
                result = conn.execute(table.select().where(
                    (table.c['reg_entidad'] == ente) & (table.c['reg_estado'] != 4) & (
                            table.c['reg_fecha_rendicion_inf'] >= fecha_desde) & (
                            table.c['reg_fecha_rendicion_inf'] < fecha_hasta) & (table.c['reg_metodo_pago'] == metodo)))
            else:
                result = conn.execute(table.select().where(
                    (table.c['reg_entidad'] == ente) & (table.c['reg_estado'] != 4) & (
                            table.c['reg_fecha_rendicion_inf'] >= fecha_desde) & (
                                table.c['reg_fecha_rendicion_inf'] < fecha_hasta)))

            df = pd.DataFrame(result.fetchall())
            if not df.empty:
                df.columns = result.keys()

        df = df.dropna(axis='columns', how='all')

        return df

    except Exception as e:
        logging.error('%s - %s', 'get_pagos_por_comision', str(e))
        raise Exception()


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


def get_registro_with_like(table_name, equal_dict, like_dict):
    try:
        metadata_obj = MetaData()
        table = Table(table_name, metadata_obj, autoload_with=engine)

        conditions = []
        for column, value in equal_dict.items():
            conditions.append(table.c[column] == value)

        for column, value in like_dict.items():
            conditions.append(table.c[column].like('%' + value))

        with engine.connect() as conn:
            result = conn.execute(table.select().where(and_(*conditions)))
            df = pd.DataFrame(result.fetchall())
            if not df.empty:
                df.columns = result.keys()

        df = df.dropna(axis='columns', how='all')

        return df
    except Exception as e:
        logging.error('%s - %s', 'get_registro_with_like', str(e))
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


def get_table(table_name):
    try:
        metadata_obj = MetaData()
        table = Table(table_name, metadata_obj, autoload_with=engine)

        df = None

        with engine.connect() as conn:
            result = conn.execute(table.select())
            df = pd.DataFrame(result.fetchall())

            if not df.empty:
                df.columns = result.keys()

        df = df.dropna(axis='columns', how='all')

        return df
    except Exception as e:
        logging.error('%s - %s', 'get_table', str(e))
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


def update_table_by_id(table_name, df_idx, column_id, set_dict):
    try:
        if len(df_idx) > 0:
            metadata_obj = MetaData()
            table = Table(table_name, metadata_obj, autoload_with=engine)
            df_idx = df_idx.rename(columns={column_id: 'id'})
            stmt = table.update().where(table.c[column_id] == bindparam('id')).values(set_dict)

            with engine.connect() as conn:
                conn.execute(stmt, df_idx.to_dict('records'))

    except Exception as e:
        logging.error('%s - %s', 'update_table_by_id', str(e))
        raise Exception()


def update_registros_by_id(table_name, df_idx, id_origen, id_destino, set_dict):
    try:
        if len(df_idx) > 0:
            metadata_obj = MetaData()
            table = Table(table_name, metadata_obj, autoload_with=engine)
            df_idx = df_idx.rename(columns={id_origen: 'id'})
            stmt = table.update().where(table.c[id_destino] == bindparam('id')).values(set_dict)

            with engine.connect() as conn:
                conn.execute(stmt, df_idx.to_dict('records'))

    except Exception as e:
        logging.error('%s - %s', 'update_table_by_id', str(e))
        raise Exception()





def insert_return_idx(table_name, df, column_return):
    try:
        metadata_obj = MetaData()
        table = Table(table_name, metadata_obj, autoload_with=engine)

        df = df.replace({np.nan: None, 'nan': None})

        inserted_idx = []

        with engine.connect() as conn:
            for _, registro in df.iterrows():
                idx = conn.execute(table.insert().returning(table.c[column_return]), registro.to_dict())
                inserted_idx.append(idx.fetchone()[0])

        return inserted_idx
    except Exception as e:
        logging.error('%s - %s', 'insert_return_idx', str(e))
        raise Exception()


def delete_registro(table_name, where_dict):
    try:
        metadata_obj = MetaData()
        table = Table(table_name, metadata_obj, autoload_with=engine)

        conditions = []
        for column, value in where_dict.items():
            conditions.append(table.c[column] == value)

        with engine.connect() as conn:
            conn.execute(table.delete().where(and_(*conditions)))

    except Exception as e:
        logging.error('%s - %s', 'update_registro_estado', str(e))
        raise Exception()


def delete_ids(df_idx, table_name, column_id):
    try:
        df_idx = df_idx.to_frame(name='b_reg_id')
        metadata_obj = MetaData()
        table = Table(table_name, metadata_obj, autoload_with=engine)
        stmt = table.delete().where(table.c[column_id] == bindparam('b_reg_id'))

        with engine.connect() as conn:
            conn.execute(stmt, df_idx.to_dict('records'))

    except Exception as e:
        logging.error('%s - %s', 'delete_ids', str(e))
        raise Exception()


def update_all(table_name, column_id, df_dato, col_dato):
    try:
        params = [{'_id' if k[0] == column_id else k[0]: k[1]
                   for k in d.items()} for d in df_dato.to_dict('records')]

        metadata_obj = MetaData()
        table = Table(table_name, metadata_obj, autoload_with=engine)
        stmt = table.update().where(table.c[column_id] == bindparam('_id')).values({col_dato: bindparam(col_dato)})

        with engine.connect() as conn:
            conn.execute(stmt, params)

    except Exception as e:
        logging.error('%s - %s', 'update_all', str(e))
        raise Exception()


def get_column_unique(table_name, where_dict, get_column):
    try:
        metadata_obj = MetaData()
        table = Table(table_name, metadata_obj, autoload_with=engine)

        conditions = []
        for column, value in where_dict.items():
            conditions.append(table.c[column] == value)

        with engine.connect() as conn:
            result = conn.execute(select([table.c[get_column]]).where(and_(*conditions)).distinct())
            df = pd.DataFrame(result.fetchall())
            if not df.empty:
                df.columns = result.keys()

        df = df.dropna(axis='columns', how='all')

        return df
    except Exception as e:
        logging.error('%s - %s', 'get_registro_by_params', str(e))
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
    
def insert_table(table_name, df=None, dict=None):
    try:
        metadata_obj = MetaData()
        table = Table(table_name, metadata_obj, autoload_with=engine)

        if df:
            df = df.replace({np.nan: None, 'nan': None})
            insert_dict = df.to_dict('records')
        else:
            insert_dict = []
            insert_dict.append(dict)

        with engine.connect() as conn:
            conn.execute(table.insert(), insert_dict)
            conn.commit()

    except Exception as e:
        logging.error('%s - %s', 'insert_table', str(e))
        raise Exception()
import sys
from gestor_reglas import GestorEjecucion, iniciar_ejecucion
#from pandas import DataFrame
import pandas as pd
from datetime import datetime
from reglas import RULES_MAP
import matplotlib.pyplot as plt


if __name__ == '__main__':
    print('Inicio', datetime.now())

    df = iniciar_ejecucion(2)
    sys.exit(0)

    # input_df = pd.DataFrame(data=[
    #     {'Comprobante': 1, 'Detalle': 'Automotor', 'Valor': 1000},
    #     {'Comprobante': 1, 'Detalle': 'Automotor', 'Valor': 2000},
    #     {'Comprobante': 1, 'Detalle': 'Inmueble', 'Valor': 5000},
    # ])
    
    # input_df = pd.read_csv('archivos/Motos.csv')

    
    try:
        gestor = GestorEjecucion(plan=5,named_inputs={'df_motos': input_df, 'df_parametros': par_df, 'df_par_gral': par_gral_df})
        gestor.ejecutar()

        res = gestor.get_salidas()
        
        print('Final', datetime.now())
         
        for key, df in res.items():
            print('\n--- Salida:', key)
            print(df)
            print('\n')
            print(df.shape)
            print('-------------------\n')
            
    except Exception as e:
        print(e)
        sys.exit(1)
import sys
from gestor_reglas import GestorEjecucion
from pandas import DataFrame
from datetime import datetime
from reglas import RULES_MAP
import matplotlib.pyplot as plt


if __name__ == '__main__':
    print('Inicio', datetime.now())

    input_df = DataFrame(data=[
        {'Comprobante': 1, 'Detalle': 'Automotor', 'Valor': 1000},
        {'Comprobante': 1, 'Detalle': 'Automotor', 'Valor': 2000},
        {'Comprobante': 1, 'Detalle': 'Inmueble', 'Valor': 5000},
    ])
    
    try:
        gestor = GestorEjecucion(input_data=input_df, input_name='input')
        
        #gestor.ejecutar_plan(2)
        gestor.ejecutar_metaplan(3)

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
import pandas as pd
import random
from datetime import datetime, timedelta


# Cantidad de filas
n = 10

def random_patente():
    letras = ''.join(random.choices("ABCDEFGHIJKLMNOPQRSTUVWXYZ", k=3))
    numeros = ''.join(random.choices("0123456789", k=3))
    return letras + numeros

def random_fecha():
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2025, 12, 31)
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    return (start_date + timedelta(days=random_days)).strftime("%d/%m/%Y")

data = []
for i in range(1, n+1):
    patente = random_patente()
    identificador = i
    valuacion_dnrpa = random.choice([None, random.randint(15000, 50000)])
    valuacion_sucerp = random.randint(15000, 50000)
    cilindrada = random.choice([150, 200, 250, 300, 500])
    base_imponible = valuacion_sucerp
    modelo = random.randint(2005, 2025)
    impuesto_anterior = random.randint(500, 2000)
    fecha_inicio = random_fecha()
    exencion = f"{random.choice([0, 10, 20])}%"
    
    data.append([
        patente, identificador, valuacion_dnrpa, valuacion_sucerp,
        cilindrada, base_imponible, modelo, impuesto_anterior,
        fecha_inicio, exencion
    ])

df = pd.DataFrame(data, columns=[
    "PATENTE", "IDENTIFICADOR", "VALUACION_DNRPA", "VALUACION_SUCERP",
    "CILINDRADA", "BASE IMPONIBLE", "MODELO", "IMPUESTO ANTERIOR",
    "FECHA INICIO", "EXENCION"
])

# Exportar a Excel
df.to_csv("Motos.csv", index=False)

print(df)

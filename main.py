import sys
from MotorReglas.gestor_reglas import GestorEjecucion, iniciar_ejecucion, ejecucion_automatica
from datetime import datetime
import pycron

@pycron.cron("* * * * * */5")
async def test(timestamp: datetime):
    ejecucion_automatica()


if __name__ == '__main__':
    print('Inicio', datetime.now())
    pycron.start()

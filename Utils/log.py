from datetime import datetime
from typing import Optional

class Log:
    def __init__(self, tipo: str, fin: datetime=None, inicio: datetime=None, regla: int=None,  mensaje: Optional[str] = None):
        self.inicio = inicio
        self.fin = fin or datetime.now()
        self.regla = regla
        self.tipo = tipo
        self.mensaje = mensaje
        self.duracion = fin - inicio if inicio is not None else None

    def to_dict(self):
        return {
            "regla": self.regla,
            "tipo": self.tipo,
            "inicio": self.inicio.isoformat(),
            "fin": self.fin.isoformat(),
            "duracion": str(self.duracion),
            "mensaje": self.mensaje,
        }

    def __repr__(self):
        return f"{self.to_dict()}"
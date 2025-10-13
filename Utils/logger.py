from typing import List
from Utils.log import Log
from datetime import datetime

class Logger:
    _instance = None


    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
           cls._instance = super(Logger, cls).__new__(cls)
           cls._instance.logs = []
           cls._instance.ejecutor = None

        return cls._instance
    
    def ensure_instance(method):
        def wrapper(cls, *args, **kwargs):
            if cls._instance is None:
                cls()
            return method(cls, *args, **kwargs)
        return wrapper

    
    @classmethod
    @ensure_instance
    def add_log(cls, log: Log):
        cls._instance.logs.append(log) 

    @classmethod
    @ensure_instance
    def info(cls, inicio: datetime, fin: datetime, regla: int):
        log = Log("INFO", fin, inicio, regla)
        cls._instance.logs.append(log)
        return log
    
    @classmethod
    @ensure_instance
    def error_regla(cls, mensaje: str, inicio: datetime=None, fin: datetime=None, regla: int=None):
        log = Log(tipo="ERROR", mensaje=mensaje, fin=fin, inicio=inicio, regla=regla)
        cls._instance.logs.append(log)
        return log
    
    @classmethod
    @ensure_instance
    def error(cls, mensaje: str):
        log = Log(tipo="ERROR", mensaje=mensaje)
        cls._instance.logs.append(log)
        return log


    @classmethod
    @ensure_instance
    def get_logs(cls) -> List[Log]:
        return cls._instance.logs

    @classmethod
    @ensure_instance    
    def clear(cls):
        cls._instance.logs = []
        cls._instance.ejecutor = None
    
    @classmethod
    @ensure_instance
    def set_ejecutor(cls, ejecutor):
        cls._instance.ejecutor = ejecutor

    @classmethod
    @ensure_instance
    def get_ejecutor(cls):
        return cls._instance.ejecutor


from datetime import datetime
from etlrules import InvalidPlanError, RuleData, RuleEngine as RuleEngineBase
from etlrules.plan import PlanMode
from etlrules.data import context
from Utils.logger import Logger

class RuleEngine(RuleEngineBase):

    def run(self, data: RuleData) -> RuleData:
        assert isinstance(data, RuleData)
        if self.plan.is_empty():
            raise InvalidPlanError("An empty plan cannot be run.")
        mode = self.plan.get_mode()
        if mode == PlanMode.PIPELINE:
            return self.run_pipeline(data)
        elif mode == PlanMode.GRAPH:
            return self.run_graph(data)
        else:
            raise InvalidPlanError("Plan's mode cannot be determined.")
        
    def run_graph(self, data: RuleData) -> RuleData:
        g = self._get_topological_sorter(data)
        g.prepare()
        with context.set(self._get_context(data)):
            while g.is_active():
                for rule_idx in g.get_ready():
                    rule = self.plan.get_rule(rule_idx)
                    inicio = datetime.now()
                    try:
                        rule.apply(data)
                        Logger.info(inicio, datetime.now(), int(rule.get_name()))
                    except Exception as e:
                        msj_error = f"Error al ejecutar regla id: {rule.get_name()}: {type(e).__name__}: {e}"
                        Logger.error_regla(msj_error, inicio, datetime.now(), int(rule.get_name()))
                        raise Exception(msj_error) from None               
                    g.done(rule_idx)
        return data
    
    # def set_logs_ejecucion(self, regla_error=None, msj_error=None):
    #     for rule in self.plan.rules:
    #         inicio = getattr(rule, "inicio", None)
    #         fin = getattr(rule, "fin", None)
    #         #Si tiene inicio y fin significa que se ejecuto y termino
    #         if inicio and fin:
    #             Logger.info(inicio, fin, int(rule.get_name()))
    #             #Si es un bloque de reglas hacer el mismo tratamiento
    #             for subregla in getattr(rule, "_rules", []):
    #                 subinicio = getattr(subregla, "inicio", None)
    #                 subfin = getattr(subregla, "fin", None)
    #                 if subinicio and subfin:
    #                     Logger.info(subinicio, subfin, int(subregla.get_name()))

    #     if regla_error:
    #         Logger.error_regla(msj_error, regla_error.inicio, regla_error.fin, int(regla_error.get_name()))
    #         for subregla in getattr(rule, "_rules", []):
    #             subinicio = getattr(subregla, "inicio", None)
    #             subfin = getattr(subregla, "fin", None)
    #             if subinicio and subfin:
    #                 Logger.info(subinicio, subfin, int(subregla.get_name()))
    #             elif subinicio and not subfin:
    #                 Logger.error_regla(msj_error, subinicio, regla_error.fin, int(subregla.get_name()))

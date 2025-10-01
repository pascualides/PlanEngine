from etlrules.backends.pandas import ProjectRule, RenameRule, SortRule, FilterRule, AddNewColumnRule, VConcatRule, DateTimeDiffRule
from etlrules.backends.pandas import IfThenElseRule, InnerJoinRule, RoundRule, RulesBlock as RulesBlockBase
from etlrules.data import RuleData
from etlrules.rule import UnaryOpBaseRule
import pandas as pd



class SumColumnsRule(UnaryOpBaseRule):
    def __init__(self, columns, named_input='main', named_output='main'):
        super().__init__()
        self.columns = columns
        self.named_input = named_input
        self.named_output = named_output

    def apply(self, data):
        super().apply(data)
        df = self._get_input_df(data)
        
        df = df[self.columns].sum().to_frame().T
        self._set_output_df(data, df)

class ConvertToDatetimeRule(UnaryOpBaseRule):
    def __init__(self, column, date_format=None, dayfirst=False, named_input=None, named_output=None):
        super().__init__()
        self.column = column
        self.date_format = date_format
        self.dayfirst = dayfirst
        self.named_input = named_input
        self.named_output = named_output
    def apply(self, data):
        super().apply(data)
        df = self._get_input_df(data)
        
        df[self.column] = pd.to_datetime(df[self.column], format=self.date_format, dayfirst=self.dayfirst, errors='coerce')
        self._set_output_df(data, df)

class MeltDataframeRule(UnaryOpBaseRule):
    def __init__(self, id_vars,  var_name, value_name, named_input=None, named_output=None):
        super().__init__()
        self.id_vars = id_vars
        self.var_name = var_name
        self.value_name = value_name
        self.named_input = named_input
        self.named_output = named_output
    def apply(self, data):
        super().apply(data)
        df = self._get_input_df(data)
        df = df.melt(id_vars=self.id_vars, var_name=self.var_name, value_name=self.value_name)
        self._set_output_df(data, df)

class StrReplaceRule(UnaryOpBaseRule):
    def __init__(self, column, pattern, replace_value, final_type=str, named_input=None, named_output=None):
        super().__init__()
        self.column = column
        self.final_type = final_type
        self.pattern = pattern
        self.replace_value = replace_value
        self.named_input = named_input
        self.named_output = named_output
    def apply(self, data):
        super().apply(data)
        df = self._get_input_df(data)
        df[self.column] = df[self.column].str.replace(self.pattern, self.replace_value).astype(self.final_type)
        self._set_output_df(data, df)


class IfThenElseCalcRule(UnaryOpBaseRule):
    def __init__(self, condition_expression, true_expr, false_expr=None,
                 named_input=None, named_output=None, output_column="RES"):
        
        super().__init__()
        self.condition = condition_expression
        self.true_expr = true_expr
        self.false_expr = false_expr
        self.named_input = named_input
        self.named_output = named_output
        self.result_column = output_column

    def apply(self, data):
        # obtengo el df
        df = self._get_input_df(data)
        # aplico condición con eval()
        mask = eval(self.condition)

        # caso verdadero
        df.loc[mask, self.result_column] = eval(self.true_expr)

        # caso falso (si está definido)
        if self.false_expr:
            df.loc[~mask, self.result_column] = eval(self.false_expr)

        # guardo salida
        data.set_named_output(self.named_output, df)


class GroupByRule(UnaryOpBaseRule):
    def __init__(self, columns, agg, named_input=None, named_output=None):
      
        super().__init__()
        self.columns = columns
        self.agg = agg
        self.named_input = named_input
        self.named_output = named_output

    def apply(self, data):
        # obtengo el df
        df = self._get_input_df(data)

        df = df.groupby(self.columns).agg(self.agg).reset_index()
        # guardo salida
        data.set_named_output(self.named_output, df)


class RulesBlock(RulesBlockBase):
    def apply(self, data):
        UnaryOpBaseRule.apply(self, data)
        data2 = RuleData(
            main_input=self._get_input_df(data),
            named_inputs={k: v for k, v in data.get_named_outputs()},
            strict=self.strict
        )
        for rule in self._rules:
            rule.apply(data2)
        self._set_output_df(data, data2.get_main_output())


RULES_MAP = {
    "SortRule": SortRule,
    "ProjectRule": ProjectRule,
    "RenameRule": RenameRule,
    "FilterRule": FilterRule,
    "AddNewColumnRule": AddNewColumnRule,
    "VConcatRule": VConcatRule,
    "SumColumnsRule": SumColumnsRule,
    "ConvertToDatetimeRule": ConvertToDatetimeRule,
    "MeltDataframeRule": MeltDataframeRule,
    "IfThenElseCalcRule": IfThenElseCalcRule,
    "GroupByRule": GroupByRule,
    "DateTimeDiffRule": DateTimeDiffRule,
    "IfThenElseRule": IfThenElseRule,
    "InnerJoinRule": InnerJoinRule,
    "RoundRule": RoundRule,
    "RulesBlock": RulesBlock,
}
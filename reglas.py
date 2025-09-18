from etlrules.backends.pandas import ProjectRule, RenameRule, SortRule, FilterRule, AddNewColumnRule, VConcatRule
from etlrules.rule import BaseRule, UnaryOpBaseRule
from pandas import DataFrame



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

RULES_MAP = {
    "SortRule": SortRule,
    "ProjectRule": ProjectRule,
    "RenameRule": RenameRule,
    "FilterRule": FilterRule,
    "AddNewColumnRule": AddNewColumnRule,
    "VConcatRule": VConcatRule,
    "SumColumnsRule": SumColumnsRule,
}
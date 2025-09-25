import sys
from etlrules import Plan, RuleEngine, RuleData
from etlrules.backends.pandas import ProjectRule, RenameRule, SortRule, FilterRule, AddNewColumnRule, TypeConversionRule, DateTimeDiffRule, IfThenElseRule, InnerJoinRule, AggregateRule, RoundRule, HConcatRule
import pandas as pd
from etlrules import RuleData
from reglas import ConvertToDatetimeRule, MeltDataframeRule, StrReplaceRule, IfThenElseCalcRule, GroupByRule
from datetime import datetime

inicio = datetime.now()
plan = Plan()


input_df = pd.read_csv('archivos/motos_menos_300.csv')
par_df = pd.read_csv('archivos/Parametros2.csv')
par_gral_df = pd.read_csv('archivos/Parametros_generales.csv')



data = RuleData(named_inputs={'df_motos': input_df, 'df_parametros': par_df, 'df_par_gral': par_gral_df})



# SOBRE Parametros
# plan.add_rule(MeltDataframeRule(id_vars=['MODELO'], var_name='CC', value_name='TOTAL_IMPUESTO', named_input='df_parametros', named_output='df_parametros_melted'))
# plan.add_rule(StrReplaceRule('CC', 'cc', '', int, named_input='df_parametros_melted', named_output='df_parametros_replaced'))
# plan.add_rule(FilterRule("df['CC'] <= 300", named_input='df_parametros_replaced', named_output='df_parametros_clean'))
#filtrar los mas 300 para hacer por menos

# FILTRO INICIAL
plan.add_rule(FilterRule("(df['CILINDRADA'] < 300) & (df['IDENTIFICADOR'] == 1)", named_input='df_motos', named_output='df_motos_filtered'))
# plan.add_rule(IfThenElseRule(condition_expression="df['VALUACION_DNRPA'] > 0", output_column='BASE_IMPONIBLE', then_column='VALUACION_DNRPA', else_column='VALUACION_SUCERP', named_input='df_motos_filtered', named_output='df_motos_base'))
# plan.add_rule(FilterRule("df['BASE_IMPONIBLE'] > 0", named_input='df_motos_base', named_output='df_motos_base_positive'))

#AGREGAR PARAMETROS GENERALES
plan.add_rule(AddNewColumnRule(column_expression='1', output_column='ID_PAR', named_input='df_par_gral', named_output='df_par_gral_id'))
plan.add_rule(AddNewColumnRule(column_expression='1', output_column='ID_PAR', named_input='df_motos_filtered', named_output='df_motos_filtered_id'))
plan.add_rule(InnerJoinRule(key_columns_left=['ID_PAR'], key_columns_right=['ID_PAR'], named_input_left='df_motos_filtered_id', named_input_right='df_par_gral_id', named_output='df_motos_parametros'))


#UNIR CON TABLA PARAMETROS MOTOS
plan.add_rule(InnerJoinRule(key_columns_left=['MODELO'], key_columns_right=['MODELO'], named_input_left='df_motos_parametros', named_input_right='df_parametros', named_output='df_joined'))
plan.add_rule(FilterRule("(df['CILINDRADA'] >= df['CILINDRADA_DESDE']) & (df['CILINDRADA'] <= df['CILINDRADA_HASTA'])", named_input='df_joined', named_output='df_joined_filtered_range'))




# plan.add_rule(FilterRule("df['CC'] >= df['CILINDRADA']", named_input='df_joined', named_output='df_joined_filtered'))
# plan.add_rule(SortRule(sort_by=['PROC_ID', 'MODELO', 'CC'], named_input='df_joined_filtered', named_output='df_joined_sorted'))
# plan.add_rule(GroupByRule(['PROC_ID', 'MODELO', 'CILINDRADA'],'first', named_input='df_joined_sorted', named_output='df_aggregated'))


#FECHAS
plan.add_rule(ConvertToDatetimeRule(column='FECHA_INICIO', dayfirst=True, named_input='df_joined_filtered_range', named_output='df_converted_fecha_inicio'))
#plan.add_rule(AddNewColumnRule('PRIMER_DIA', "'01/01/2025'", named_input='df_converted_fecha', named_output='df_con_primer_dia'))
plan.add_rule(ConvertToDatetimeRule(column='PRIMER_DIA', dayfirst=True, named_input='df_converted_fecha_inicio', named_output='df_con_primer_dia_converted'))
#plan.add_rule(AddNewColumnRule('ULTIMO_DIA', "'31/12/2025'", named_input='df_con_primer_dia_converted', named_output='df_con_ultimo_dia'))
plan.add_rule(ConvertToDatetimeRule(column='ULTIMO_DIA', dayfirst=True, named_input='df_con_primer_dia_converted', named_output='df_con_ultimo_dia_converted'))
plan.add_rule(DateTimeDiffRule(input_column='ULTIMO_DIA', input_column2='FECHA_INICIO', unit='days',output_column='DIAS_ULTIMO', named_input='df_con_ultimo_dia_converted', named_output='df_con_dias_fin'))
plan.add_rule(DateTimeDiffRule(input_column='FECHA_INICIO', input_column2='PRIMER_DIA', unit='days',output_column='DIAS', named_input='df_con_dias_fin', named_output='df_con_diferencia_dias'))



#CALCULOS
plan.add_rule(IfThenElseCalcRule(condition_expression="df['FECHA_INICIO'] <= df['PRIMER_DIA']", 
                                 false_expr="df['DIAS_ULTIMO'] * (df['VALOR'] / 365)", 
                                 true_expr="df['VALOR']", output_column='TOTAL_IMPUESTO', 
                                 named_input='df_con_diferencia_dias', named_output='df_total_calculado'))

plan.add_rule(RoundRule(input_column='TOTAL_IMPUESTO', 
                        scale=2, 
                        named_input='df_total_calculado', named_output='df_total_rounded'))

plan.add_rule(IfThenElseCalcRule(condition_expression="(df['IMPUESTO_ANTERIOR'] > 0) & (df['TOTAL_IMPUESTO'] > (df['IMPUESTO_ANTERIOR'] * df['TOPE_MAXIMO']))", 
                                 true_expr="(df['TOTAL_IMPUESTO'] - (df['IMPUESTO_ANTERIOR'] * df['TOPE_MAXIMO'])) * (-1)", 
                                 false_expr="0", output_column='TOTAL_TOPE_935', 
                                 named_input='df_total_rounded', named_output='df_tope_935'))

plan.add_rule(RoundRule(input_column='TOTAL_TOPE_935', 
                        scale=2, 
                        named_input='df_tope_935', named_output='df_tope_935_rounded'))

plan.add_rule(IfThenElseCalcRule(condition_expression="df['EXENCION'] > 0", 
                                 true_expr="(df['TOTAL_IMPUESTO'] + df['TOTAL_TOPE_935']) * df['EXENCION'] * -1", 
                                 false_expr="0", 
                                 output_column='TOTAL_EXENCION', named_input='df_tope_935_rounded', named_output='df_total_exencion'))



plan.add_rule(RoundRule(input_column='TOTAL_EXENCION', 
                        scale=2, 
                        named_input='df_total_exencion', named_output='df_total_exencion_rounded'))



plan.add_rule(AddNewColumnRule(output_column='TOTAL_DESCUENTO_163', 
                               column_expression="(df['TOTAL_IMPUESTO'] + df['TOTAL_TOPE_935'] + df['TOTAL_EXENCION']) * df['DESCUENTO_ANUAL'] * (-1)",
                               named_input='df_total_exencion_rounded', named_output='df_total_descuento_163'))



plan.add_rule(RoundRule(input_column='TOTAL_DESCUENTO_163', 
                        scale=2, 
                        named_input='df_total_descuento_163', named_output='df_total_descuento_163_rounded'))


plan.add_rule(AddNewColumnRule(output_column='SALDO_POSICION', 
                               column_expression="df['TOTAL_IMPUESTO'] + df['TOTAL_TOPE_935'] + df['TOTAL_EXENCION'] + df['TOTAL_DESCUENTO_163']"
                               ,named_input='df_total_descuento_163_rounded', named_output='df_saldo_posicion'))

plan.add_rule(RoundRule(input_column='SALDO_POSICION', 
                        scale=2, 
                        named_input='df_saldo_posicion', named_output='df_final'))


rule_engine = RuleEngine(plan)
rule_engine.run(data)
result = data.get_named_output('df_final')  

result.to_csv('archivos/Procesados.csv',sep=';', index=False)
#result = result.loc[result['PROC_ID'] == 14928013]

#result = data.get_main_output()
print(result[['PROC_ID', 'FECHA_INICIO', 'PRIMER_DIA', 'TOTAL_IMPUESTO', 'IMPUESTO_ANTERIOR', 'TOTAL_TOPE_935', 'TOTAL_EXENCION', 'TOTAL_DESCUENTO_163', 'SALDO_POSICION']])

print('Inicio', inicio)
fin = datetime.now()
print('Fin', fin)
print ('Resta', fin - inicio)

create table public."Plan" (
	plan_id serial4 not null,
	plan_name text not null,
	plan_orden int4 null,
	plan_meta_id serial4 null,
	constraint "Plan_pk" primary key (plan_id)
	constraint "Plan_fk_meta_plan" foreign key ("plan_meta_id") references public."Metaplan"(met_id) on delete set null on update cascade,
);

create table public."Tipo_Regla" (
	tip_reg_id serial4 not null,
	tip_reg_nombre text not null,
	tip_reg_nombre_int text not null,
	constraint "Tipo_regla_pk" primary key (tip_reg_id)
);

create table public."Reglas" (
	reg_id serial4 not null,
	reg_orden int4 not null,
	reg_tipo_regla int4 not null,
	reg_plan_id int4 not null,
	reg_nombre text null,
	constraint "Regla_pk" primary key (reg_id),
	constraint "Regla_fk_tipo_regla" foreign key ("reg_tipo_regla") references public."Tipo_Regla"(tip_reg_id) on delete set null on update cascade,
	constraint "Regla_fk_plan" foreign key ("reg_plan_id") references public."Plan"(plan_id) on delete set null on update cascade
);

create table public."Tipo_Parametros" (
	tip_par_id serial4 not null,
	tip_par_nombre text not null,
	tip_par_nombre_int text not null,
	tip_par_tipo text not null,
	constraint "Tipo_parametro_pk" primary key (tip_par_id)
);

create table public."Parametros" (
	par_id serial4 not null,
	par_reg_id serial4 not null,
	par_tipo_par serial4 not null, 
	constraint "Parametro_pk" primary key (par_id),
	constraint "Parametro_fk_regla" foreign key ("par_reg_id") references public."Reglas"(reg_id) on delete set null on update cascade,
	constraint "Parametro_fk_tipo_par" foreign key ("par_tipo_par") references public."Tipo_Parametros"(tip_par_id) on delete set null on update cascade
);

create table public."Valores" (
	val_id serial4 not null,
	val_llave text null,
	val_valor text not null,
	val_param_id serial4 not null,
	constraint "Valor_pk" primary key (val_id),
	constraint "Valor_fk_parametro" foreign key ("val_param_id") references public."Parametros"(par_id) on delete set null on update cascade
);

create table public."Salidas" (
	sal_id serial4 not null,
	sal_nombre text not null,
	sal_nombre_df text not null,
	sal_plan_id serial4 not null,
	constraint "Salida_pk" primary key (sal_id),
	constraint "Salida_fk_plan" foreign key ("sal_plan_id") references public."Plan"(plan_id) on delete set null on update cascade
);

create table public."Metaplan" (
	met_id serial4 not null,
	met_nombre text not null,
	constraint "Metaplan_pk" primary key (met_id)
);

create table public."MetaplanXplan" (
	mxp_id serial4 not null,
	mxp_plan_id int4 not null,
	mxp_meta_id int4 not null,
	mxp_orden int4 not null,
	constraint "MetXplan_pk" primary key (mxp_id),
	constraint "MetXplan_fk_plan" foreign key ("mxp_plan_id") references public."Plan"(plan_id) on delete set null on update cascade,
	constraint "MetXplan_fk_meta" foreign key ("mxp_meta_id") references public."Metaplan"(met_id) on delete set null on update cascade
);

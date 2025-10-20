create table public."Plan" (
	plan_id serial4 not null,
	plan_name text not null,
	constraint "Plan_pk" primary key (plan_id)
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
	reg_bloque_regla_id serial4 null,
	constraint "Regla_pk" primary key (reg_id),
	constraint "Regla_fk_tipo_regla" foreign key ("reg_tipo_regla") references public."Tipo_Regla"(tip_reg_id) on delete set null on update cascade,
	constraint "Regla_fk_plan" foreign key ("reg_plan_id") references public."Plan"(plan_id) on delete set null on update cascade,
	constraint "Regla_fk_bloque_regla" foreign key ("reg_bloque_regla_id") references public."Reglas"(reg_id) on delete set null on update cascade
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
	sal_ejec_id serial4 not null,
	constraint "Salida2_pk" primary key (sal_id),
	constraint "Salida_fk_ejecutor" foreign key ("sal_ejec_id") references public."Ejecutor"(ejec_id) on delete set null on update cascade
);

create table public."Tipos_Estado" (
	tes_id serial4 not null,
	tes_nombre text not null,
	tes_descripcion text null,
	constraint "Tipos_estado_pk" primary key (tes_id)
);


create table public."Ejecutor" (
	ejec_id serial4 not null,
	ejec_id_plan serial4 not null,
	ejec_id_tes serial4 not null,
	ejec_fecha timestamp not null default now(),
	ejec_iniciar_desde int4 null,
	ejec_reintento_de int4 null,
	constraint "Ejecutor_id" primary key (ejec_id),
	constraint "Ejecutor_fk_plan" foreign key (ejec_id_plan) references public."Plan"(plan_id) on delete set null on update cascade,
	constraint "Ejecutor_fk_tes" foreign key (ejec_id_tes) references public."Tipos_Estado"(tes_id) on delete set null on update cascade
);

create table public."Estados" (
	est_id serial4 not null,
	est_id_tipo serial4 not null,
	est_fecha timestamp not null default now(),
	est_id_ejec serial4 not null,
	constraint "Estados_pk" primary key (est_id),
	constraint "Estados_fk_tipos" foreign key ("est_id_tipo") references public."Tipos_Estado"(tes_id) on delete set null on update cascade,
	constraint "Estados_fk_ejecutor" foreign key ("est_id_ejec") references public."Ejecutor"(ejec_id) on delete set null on update cascade
);


create table public."Entradas" (
	ent_id serial4 not null,
	ent_id_ejecutor serial4 not null,
	ent_ubicacion text not null,
	ent_nombre text not null,
	constraint "Entradas_pk" primary key (ent_id),
	constraint "Entradas_fk_ejecutor" foreign key ("ent_id_ejecutor") references public."Ejecutor"(ejec_id) on delete set null on update cascade
);

create table public."Logs" (
	log_id serial4 not null,
	log_tipo text not null,
	log_msg text null,
	log_fecha_inicio timestamp null,
	log_fecha_fin timestamp null,
	log_duracion text null,
	log_id_regla int4 null,
	log_id_ejecutor int4 not null,
	log_dim_entrada text null,
	log_dim_salida text null,
	constraint "Logs_pk" primary key (log_id)
);
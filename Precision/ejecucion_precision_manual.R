
library(tidyverse)
library(glue)
library(RPostgres)

fecha_resumen <- format(Sys.time(),"%Y-%m-%d %H:%M:%S")

drv <- Postgres()

conexion <- dbConnect(
  drv = drv,
  dbname = "procesamiento",
  host = "192.168.29.70",
  port = 5432,
  user = "automatiza01",
  password = "STECSDI_2022.auto"
)



tabla_prec <- dbGetQuery(conexion,
                         "SELECT 
                          tabla_entrada, 
                          variables_tabla, 
                          concepto FROM st09_precision") %>%
  filter(grepl('^me06_escolarizados_nor', tabla_entrada))

tabla <- unique(tabla_prec$tabla_entrada)

query_pob <- ifelse(tabla == 'rc02_revit_nor',
                    "SELECT p_cedula, p_nombres, p_apellidos, p_nombres_apellidos, p_sexo, p_fecha_nac, p_identidad_mad, p_identidad_pad FROM st05_registro_poblacion WHERE substring(id_hash_rp,1,5)=\\\'00101\\\'",
                    "SELECT p_cedula, p_nombres, p_apellidos, p_nombres_apellidos, p_sexo, p_fecha_nac, p_identidad_mad, p_identidad_pad FROM st05_registro_poblacion")
if (length(tabla)!=1) stop("Verificar insumo, hay más de una tabla a la vez")

# Insumos de Reporteria ---

dbExecute(
  conexion,
  glue('INSERT INTO "logs"."log_ratore" (tabla,script,proceso,fecha,nobs,error,id_table,id_proceso,codigo_ejecucion,id_ejecucion) VALUES
        (\'{tabla}\',\'05\',\'inicio\',\'{fecha_resumen}\',NULL,NULL,NULL,NULL,NULL,NULL);'))

# Arma el query ----
# verifica que existan las variables

no_existen <- 
  setdiff(
    tabla_prec$variables_tabla,
    dbListFields(conexion,tabla)
  )

if (!is_empty(no_existen)) {
  tabla_query <-
    tabla_prec %>%
    mutate(
      variables_tabla=
        case_when(
          concepto=='p_nombres' & ('p_nombres' %in% pull(filter(tabla_prec,variables_tabla %in% no_existen),concepto)) ~ 'NULL',
          concepto=='p_apellidos' & ('p_apellidos' %in% pull(filter(tabla_prec,variables_tabla %in% no_existen),concepto)) ~ 'NULL',
          concepto=='p_sexo' & ('p_sexo' %in% pull(filter(tabla_prec,variables_tabla %in% no_existen),concepto)) ~ '\\\'2\\\'',
          T ~ variables_tabla
        )
    )
}else{
  tabla_query <- tabla_prec
}

# Chequea si ya tiene filas procesadas 

check_nm <- "id_etiqueta" %in% dbListFields(conexion,tabla)

if(check_nm){
  cod_lectura <- "WHERE id_etiqueta=\\\'no encontrado\\\' OR id_etiqueta IS NULL"
}else{
  cod_lectura <- ""
}

query_pivot <-
  paste0(
    tabla_query$variables_tabla,
    " AS ",
    tabla_query$concepto,collapse = ', ') %>%
  glue("SELECT {vars}, id_hash FROM {tabla} {cod_lectura} ",vars=.)

# query_pivot <-
#   paste0("a.",
#     tabla_query$variables_tabla,
#     " AS ",
#     tabla_query$concepto, collapse = ', '
#   ) %>%
#   glue("SELECT {vars}, id_hash FROM {tabla} a INNER JOIN st05_registro_poblacion b ON a.p_cedula_nac = b.p_cedula LIMIT 100", vars=.)

# Definición de variables de identificación necesarias
listaint <-
  if('p_nombres_apellidos' %in% tabla_prec$concepto |
     all(c('p_nombres','p_apellidos') %in% tabla_prec$concepto)){
    tabla_prec %>%
      filter(!is.na(concepto)) %>%
      pull(concepto) %>% 
      c(.,"iniciales","p_nombres_apellidos") %>%
      unique() %>% 
      str_c("\'",.,"\'", collapse = ",")
  }else{
    tabla_prec %>%
      filter(!is.na(concepto)) %>%
      pull(concepto) %>% 
      str_c("\'",.,"\'", collapse = ",")
  }

# Declaración de variables de entorno
source(file = "/home/hadoop/RA_to_RE/script/declaracion_variables_entorno.R", encoding = "UTF-8")

setwd("/home/hadoop/spark/bin/")

str_c(
  "./spark-submit --master spark://192.168.29.90:7077 ",
  "--conf spark.driver.host=192.168.29.91 ",
  "--conf spark.sql.files.maxPartitionBytes=402653184 ",
  "--driver-memory 20g ",
  "--executor-memory 22g ",
  "/home/hadoop/RA_to_RE/script/algoritmo_precision_spark.py ",
  "\"{'data_pi_query':",
  "'", query_pivot, "'",
  ",'data_pob_query':",
  "'", query_pob, "'",
  ",'listaint':[",
  listaint,
  "],'opborrar':False,",
  "'tabla_pi':","'",tabla,"'",
  "}\""
) %>% system(command = .)

# Verifica la existencia de id_etiqueta y estado_identidad en la tabla final
new_vars_prec <- c("id_etiqueta","estado_identidad")[!c("id_etiqueta","estado_identidad") %in% dbListFields(conexion,tabla)]

if(!"p_nombres_apellidos" %in% dbListFields(conexion,tabla)){
  new_vars_prec <- c(new_vars_prec,"p_nombres_apellidos")
}

# Si no existen nombres y apellidos juntos en el insumo la crea para la lista
if(!"p_nombres_apellidos" %in% tabla_prec$concepto){
  tabla_prec[nrow(tabla_prec)+1,] <- c(tabla,"p_nombres_apellidos","p_nombres_apellidos")
}
#
# # Crea las variables que no existen en la tabla final
if(isTRUE(!is_empty(new_vars_prec))){
  map(new_vars_prec,~ dbExecute(conexion,glue("ALTER TABLE {tabla} ADD COLUMN {.x} text;")))
}
#
# # Vector de character que empata los nombres finales de la tabla de resultado y los conceptos
vec_names <-
  c('p_idr','p_nombres_reg','p_apellidos_reg','p_nombres_apellidos_reg') %>%
  set_names(c('p_cedula','p_nombres','p_apellidos','p_nombres_apellidos'))
#
# # En el insumo aumenta los nombres de la tabla resultado con los de los conceptos
tabla_prec <-
  tabla_prec %>%
  filter(concepto %in% c('p_cedula','p_nombres','p_apellidos','p_nombres_apellidos')) %>%
  mutate(nombre_preci=vec_names[concepto])

# Cuenta si las tablas de resultado son mayores a 0
conteos_prec <-
  paste0(tabla,
         c("mach_join",
           "mach_pivot",
           "validados",
           "residuales")) %>%
  set_names(.,.) %>%
  map(~{
    if(dbExistsTable(conexion,Id(schema = 'precision', table = .x))) {
      (dbGetQuery(conexion,glue('SELECT COUNT(*) AS conteo FROM \"precision\".\"{.x}\"')) %>% pull(conteo))>0
    }else{
      FALSE
    }

  })

fecha_guardado <- format(Sys.time(),"%Y-%m-%d %H:%M:%S")

dbExecute(
  conexion,
  glue('INSERT INTO "logs"."log_ratore" (tabla,script,proceso,fecha,nobs,error,id_table,id_proceso,codigo_ejecucion,id_ejecucion) VALUES
        (\'{tabla}\',\'05\',\'guardado\',\'{fecha_guardado}\',NULL,NULL,NULL,NULL,NULL,NULL);'))

# Validados ----
#
if(pluck(conteos_prec,
         paste0(tabla,"validados"))){

  sintax_update <-
    paste0(tabla_prec$variables_tabla,"=p.",tabla_prec$nombre_preci,collapse = ",\n") %>%
    paste0("id_etiqueta=p.id_etiqueta,\nestado_identidad=\'validados\',\n",.)

  dbExecute(
    conexion,
    glue(
      'UPDATE public.{tabla} o
    SET {sintax_update}
    FROM precision.{name_prec} p
    WHERE o.id_hash=p.id_hash;',
    name_prec=paste0(tabla,'validados')
    )
  )
}
#
# # No encontrado ----
#
if(pluck(conteos_prec,
         paste0(tabla,"mach_pivot"))){

  sintax_update <- "id_etiqueta=p.id_etiqueta,\nestado_identidad=\'no encontrado\'\n"

  dbExecute(
    conexion,
    glue(
      'UPDATE public.{tabla} o
    SET {sintax_update}
    FROM precision.{name_prec} p
    WHERE o.id_hash=p.id_hash;',
    name_prec=paste0(tabla,'mach_pivot')
    )
  )
}
#
# # Determinístico ----
#
if(pluck(conteos_prec,
         paste0(tabla,"mach_join"))){

  sintax_update <-
    paste0(tabla_prec$variables_tabla,"=p.",tabla_prec$nombre_preci,collapse = ",\n") %>%
    paste0("id_etiqueta=p.id_etiqueta,\nestado_identidad=\'recuperado\',\n",.)


  dbExecute(
    conexion,
    glue(
      'UPDATE public.{tabla} o
    SET {sintax_update}
    FROM precision.{name_prec} p
    WHERE
    o.id_hash=p.id_hash AND
    (p.p_nombres_reg IS NOT NULL AND
     p.p_apellidos_reg IS NOT NULL);',
    name_prec=paste0(tabla,'mach_join')
    )
  )

  sintax_update <-
    paste0(pull(filter(tabla_prec,nombre_preci %in% c('p_idr','p_nombres_apellidos_reg')),variables_tabla),
           "=p.",
           pull(filter(tabla_prec,nombre_preci %in% c('p_idr','p_nombres_apellidos_reg')),nombre_preci),
           collapse = ",\n") %>%
    paste0("id_etiqueta=p.id_etiqueta,\nestado_identidad=\'recuperado\',\n",.)


  dbExecute(
    conexion,
    glue(
      'UPDATE public.{tabla} o
      SET {sintax_update}
      FROM precision.{name_prec} p
      WHERE
      o.id_hash=p.id_hash AND
      (p.p_nombres_reg IS NULL AND
       p.p_apellidos_reg IS NULL);',
      name_prec=paste0(tabla,'mach_join')
    )
  )
}
#
#
# # Fuzzy/Difuso ----
#
if(pluck(conteos_prec,
         paste0(tabla,"residuales"))){

  sintax_update <-
    paste0(tabla_prec$variables_tabla,"=p.",tabla_prec$nombre_preci,collapse = ",\n") %>%
    paste0("id_etiqueta=p.id_etiqueta,\nestado_identidad=\'residuales\',\n",.)


  dbExecute(
    conexion,
    glue(
      'UPDATE public.{tabla} o
    SET {sintax_update}
    FROM precision.{name_prec} p
    WHERE
    o.id_hash=p.id_hash AND
    (p.p_nombres_reg IS NOT NULL AND
     p.p_apellidos_reg IS NOT NULL);',
    name_prec=paste0(tabla,'residuales')
    )
  )

  sintax_update <-
    paste0(pull(filter(tabla_prec,nombre_preci %in% c('p_idr','p_nombres_apellidos_reg')),variables_tabla),
           "=p.",
           pull(filter(tabla_prec,nombre_preci %in% c('p_idr','p_nombres_apellidos_reg')),nombre_preci),
           collapse = ",\n") %>%
    paste0("id_etiqueta=p.id_etiqueta,\nestado_identidad=\'residuales\',\n",.)


  dbExecute(
    conexion,
    glue(
      'UPDATE public.{tabla} o
      SET {sintax_update}
      FROM precision.{name_prec} p
      WHERE
      o.id_hash=p.id_hash AND
      (p.p_nombres_reg IS NULL AND
       p.p_apellidos_reg IS NULL);',
      name_prec=paste0(tabla,'residuales')
    )
  )
}

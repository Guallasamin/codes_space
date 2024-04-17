
# ------------------------------------------------------------------------- #
#      Declaración de variables de entorno para la conexión al cluster      #
# ------------------------------------------------------------------------- #

# Posterior a la extracción de variables de entorno de la sesión de R en la 
# terminal.

# Librerias ---------------------------------------------------------------


library(tidyverse)
library(rlang)
library(glue)

# Lectura de las variables de entorno de la termianl ----------------------

var_env <- read_rds("/home/hadoop/RA_to_RE/insumos/var_env.rds")


# Creación de las sentencias de declaración  ------------------------------


variables_entorno <- var_env %>%
  mutate(
    expresion = glue("Sys.setenv('{key}' = '{valor}')")
  ) %>%
  pull(expresion)


# Ejecución de las sentencias ---------------------------------------------

variables_entorno %>%
  map(parse_expr) %>%
  map(eval)


# Reejecución -------------------------------------------------------------

# Dado que algunas variables de entorno están parametrizadas, se recomienda
# ejecutar las siguientes variables de entorno por segunda ocasión:

Sys.setenv(HADOOP_CONF_DIR = '/home/hadoop/hadoop/etc/hadoop/')
Sys.setenv(YARN_CONF_DIR = '/home/hadoop/hadoop/etc/hadoop/')
Sys.setenv("JAVA_HOME" = "/usr/lib/jvm/java-1.8.0-openjdk-amd64/jre")

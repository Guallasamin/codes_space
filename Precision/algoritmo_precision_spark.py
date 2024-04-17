import sys
import json
import findspark
findspark.init()
import pyspark
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import col
from pyspark.sql.functions import greatest
from collections import Counter
from math import ceil
from math import sqrt

if __name__ == "__main__":
    if len(sys.argv) != 1:
        print("---> Como hay más de un argumento del sistema, se usará solo diccionario como argumento: ", file=sys.stderr)
        int_dic=eval(sys.argv[1])
        print(int_dic.keys())
          
          # Iniciar sesión de spark en conjunto con postgresql:
          
        data_pi_nam = int_dic['data_pi_query']
        data_pob_nam = int_dic['data_pob_query']
        
        spark = SparkSession.builder\
              .appName("Precision "+data_pi_nam)\
              .getOrCreate()
                
        path_location= "hdfs:///tmp/guardar/"
        #path_location= "/media/henryx/alldata/algoritmo_pre/datos/allmillon/final"
        
        # Extracción de elementos de los diccionarios 
        
        name_tlb = int_dic['tabla_pi']
        opcionborrar = int_dic['opborrar']
        variables_into = int_dic['listaint']
        
        print("************************************ INICIO LECTURA TABLAS ************************************")
          
        reg_pobl = spark.read.format("jdbc") \
              .option("url", "jdbc:postgresql://192.168.29.70:5432/procesamiento") \
              .option("driver", "org.postgresql.Driver") \
              .option("query",data_pob_nam) \
              .option("user", "automatiza01")\
              .option("password", "STECSDI_2022.auto")\
              .load()
              
              # .option("query", "SELECT * FROM st05_registro_poblacion") \
              # .option("dbtable", "st05_registro_poblacion") \
              
         
        data_piv = spark.read.format("jdbc") \
              .option("url", "jdbc:postgresql://192.168.29.70:5432/procesamiento") \
              .option("driver", "org.postgresql.Driver") \
              .option("query", data_pi_nam) \
              .option("user", "automatiza01")\
              .option("password", "STECSDI_2022.auto")\
              .load()
              

                
        #parametros que se pueden añadi
        # reg_pobl.show()
        # data_piv.show()
        Id_reg = 'p_idr'
        Id_piv = 'p_idp'
        ##  spark.sparkContext.setCheckpointDir
        # permite almacenar los datos salientes del proceso deterministico
        # diminuyendo el tiempo de pocesamiento 
        
        spark.sparkContext.setCheckpointDir(path_location+'cheakpoint/')
       # saprk.conf.set()
       # spark.conf.get("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
       
        variables_de_tiempo = {'p_fecha_nac', 'p_fecha_de_fatte'}
        
        variables_reg = set( reg_pobl.columns)
       
        
        """  Area de las funciones"""
        
        
        
        #verificamos si realmente las varibles ingresadas existes en la base pivot.
        variables_int1= []
        for i in variables_into:
            if  i in data_piv.columns:
                variables_int1.append(i)
        
        
        
        
        # ya con las variables obtenidas realizamos un mach con las variables del registro
        
        variables_int= []
        for i in variables_int1:
            if  i in variables_reg:
                variables_int.append(i)
             
        
        """ Agregamos identificadores a los registros target y pivot  """
        
        
        reg_pobl = reg_pobl.withColumn('p_idr',reg_pobl.p_cedula) #Se escoge la cédula como identificador del target
         
        
        
        wa = Window().orderBy(F.lit('A')) #variable sin importancias
        # para le registro pivot se escojen numeros segun su indice.
        data_piv = data_piv.withColumn(Id_piv, F.row_number().over(wa))
        data_piv = data_piv.withColumn(Id_piv, F.col(Id_piv).cast(StringType() ))
        data_piv = data_piv.select([Id_piv]+variables_int+['id_hash']) 
        
        # El tamaño de las particiones debe ser proporcional al numero de nucleos disponibles:
        # 3 workers
        # 6 nucleos/worker
        # 15 particiones por cada nucleo 
        
        # # Agregados el 5 de septiembre de 2023
        # # Calcular el tamaño del DataFrame en bytes
        # tamano_mb_pobl = sys.getsizeof(reg_pobl)/1024**2
        # n_par_pobl = ceil(tamano_mb_pobl/100)
        # tamano_mb_piv = sys.getsizeof(data_piv)/1024**2
        # n_par_piv = ceil(tamano_mb_piv/100)
        # 
        # n_par_pobl = estimar_particiones.calcular_particiones(n_par_pobl, 6)
        # n_par_reg = estimar_particiones.calcular_particiones(n_par_piv, 6)
        # 
        # # # antes
        # # # reg_pobl = reg_pobl.repartition(480)
        # # # data_piv = data_piv.repartition(480)
        # reg_pobl = reg_pobl.repartition(n_par_pobl)
        # data_piv = data_piv.repartition(n_par_reg)
        
        """agregamos nombres y apellidos juntos en caso de se necesario"""
        # F.udf crea una  funcion similar a map en la libreria pandas
        @F.udf 
        def join_apelidos( nom_ape, nom_ape_pi):
            if nom_ape:
                return nom_ape
            else:
             return nom_ape_pi
         
        if  ('p_nombres' in data_piv.columns) and ('p_apellidos' in data_piv.columns): 
               data_piv=data_piv.select('*',F.concat_ws(' ',\
                           data_piv.p_apellidos, data_piv.p_nombres ).alias('na_ap_pivot'))
            
               if 'p_nombres_apellidos' in  data_piv.columns:
                 data_piv = data_piv.withColumn('p_nombres_apellidos', \
                    join_apelidos(data_piv.p_nombres_apellidos ,data_piv.na_ap_pivot))
                 data_piv =data_piv.drop('na_ap_pivot' )
               else:
                   data_piv=data_piv.withColumnRenamed('na_ap_pivot', 'p_nombres_apellidos')
        else:
                print("No existen nombres o apellidos")
               
        
        
        
        
        """ Dar iniciales registro no tiene iniciales """
        
        @F.udf
        def dar_iniciales(fullname):
            if fullname:
              name_list = fullname.split()
              initials = ""
              for name in name_list:  # va por cada elemento en grupo.
                 initials += name[0].upper()  # agrega inicial
            
              return initials
            else:
                fullname
            
        if "iniciales" in data_piv.columns:
             None
        else:
                 data_piv= data_piv.withColumn('iniciales', \
                     dar_iniciales(data_piv.p_nombres_apellidos))
                     
        if "iniciales" in reg_pobl.columns:
             None
        else:
                 reg_pobl= reg_pobl.withColumn('iniciales', \
                     dar_iniciales(reg_pobl.p_nombres_apellidos))
           
                
        """ Definiendo variables finales"""
        
        
        
        dic_var_deter = { "id_cedula_nombre_apell":       ['p_cedula','p_nombres','p_apellidos'],\
                          "id_cedula_nombreyapllido":     ['p_cedula','p_nombres_apellidos'],\
                          "id_cedula_sexo_fna":           ['p_cedula','p_sexo','p_fecha_nac'],\
                          "id_nombre_apell_sexo_fnac":    ['p_nombres', 'p_apellidos','p_sexo','p_fecha_nac'],\
                          "id_nombreyapell_sexo_fnac":    ['p_nombres_apellidos','p_sexo','p_fecha_nac'],\
                          "id_cedula_iniciales_sexo":     ['p_cedula','iniciales','p_sexo'],\
                          "id_apell_sexo_fnac":           ['p_apellidos','p_sexo','p_fecha_nac'],\
    
                          
                          "id_cedulma_fna_sexo":          ['p_cedula_mad','p_fecha_nac','p_sexo'],\
                          "id_cedulpa_fna_sexo":          ['p_cedula_pad','p_fecha_nac','p_sexo'],\
                          "id_cedulma_nombre_apell":      ['p_cedula_mad', 'p_nombres', 'p_apellidos'],\
                          "id_cedulma_nombreyapell":      ['p_cedula_mad', 'p_nombres_apellidos'],\
                          "id_cedulpa_nombre_apell":      ['p_cedula_pad', 'p_nombres', 'p_apellidos'],\
                          "id_cedulpa_nombreyapell":      ['p_cedula_pad', 'p_nombres_apellidos'],\
                          "id_cedulma_iniciale_sexo":     ['p_cedula_mad','iniciales','p_sexo'],\
                          "id_cedulpa_iniciale_sexo":     ['p_cedula_pad','iniciales','p_sexo'],\
                          "id_cedulma_apell":             ['p_cedula_mad', 'p_apellidos'],\
                          "id_cedulpa_apell":             ['p_cedula_pad', 'p_apellidos']
                          
                          }   
        
            
            
            
            
            
        dic_var_difu = { "idif_sexo_fna_N_A":       [['p_sexo','p_fecha_nac'], \
                                ['p_nombres', 'p_apellidos','p_nombres_apellidos'],0],\
                        
                        "idif_sexo_fna_N_A_30":     [['p_sexo','p_fecha_nac'], \
                                ['p_nombres', 'p_apellidos','p_nombres_apellidos'],30]}                
                       
                      
            
        """ceacion de dicionario a partir de las variables obtenidas"""
        
        
        def f_var_det_convi(dic_var_dete,dic_var_difu, variables_int):
            if variables_int:
                X= set(variables_int)  
                Y = {}
                Z = {}
                for i in  dic_var_dete.keys():
                   if  set(dic_var_dete[i]).issubset(X)  :
                       Y[i]= dic_var_dete[i] 
                
                for i in  dic_var_difu.keys():
                   if  set(dic_var_difu[i][0]+dic_var_difu[i][0] ).issubset(X)  :
                       Z[i]= dic_var_difu[i] 
                
                return  Y, Z
            else:
                print('Variables no definidas')
        
            
         
        
        dic_var_det_convi, dic_var_dif_convi = f_var_det_convi(dic_var_deter,dic_var_difu, data_piv.columns  )
            
           
        
        
        """ALGORITMO DEL COSENO"""
        
        def split_fun(word):
            # permite crear los n-grmas de los conjuntos
            li=[]
            for i in range(len(word)-1):
                li.append(word[i]+word[i+1] )
            return li
        def word2vec(word):
            #cuenta los caracteres de una una palabra
            cw = Counter(word)
            # transforma en un conjunto de carateres
            sw = set(cw)
            # precomputes the "length" of the word vector
            lw = sqrt(sum(c*c for c in cw.values()))
        
            # return a tuple
            return cw, sw, lw
        
        
        def cosdis(v1, v2):
            if v1 and v2:
                v1=word2vec(split_fun(v1))
                v2=word2vec(split_fun(v2))
                # palablas en commun
                #common = v1[1].intersection(v2[1])
                # por la definicion del coseo tenemos que 
                if v1[2]*v2[2]==0:
                   return 0.8 
               	else: 
                   common= v1[1].intersection(v2[1])
                   return  1-abs(sum(v1[0][i]*v2[0][i] for i in common)/v1[2]/v2[2])
            else: 
                return 0.8
        
        g_funt = F.udf(cosdis, FloatType())
        # cosdis("casa","mesa")
        
        
        
        
        """coseno2"""
        def codisa(x,Y):
            X_set = set(X)
            Y_set = set(Y)
            rvector = X_set.union(Y_set) 
            for w in rvector:
                if w in X_set: l1.append(1) # create a vector
                else: l1.append(0)
                if w in Y_set: l2.append(1)
                else: l2.append(0)
            c = 0
              
            # cosine formula 
            for i in range(len(rvector)):
                    c+= l1[i]*l2[i]
            cosine = c / float((sum(l1)*sum(l2))**0.5)
            return cosine
        
        
        """ la funciones complementarias """
        # la funcion unico cuenta sobre las particiones
        # y elimina aquellos valores que se repiten dos veces
        #  en la variable "p_idr" variable del registro
        def unico(dat_tjo):
            return dat_tjo.selectExpr(
              '*', 
              'count(*) over (partition by p_idr) as cnt'
            ).filter(F.col('cnt') == 1).drop('cnt')
        
           
        # la funcion tim_df, toma como entrada un data frame
        # y devuelve un  dataframe con las
        # variables de fecha tipo to_timestamp()
        # cuyas variables_de_tiempo deben estar definidas
        # previamente. 
        def tim_df( df ):
             for i in variables_de_tiempo:
                 if i in df.columns:
                   df=  df.withColumn(i,F.regexp_replace(F.col(i), r"\d\d:\d\d:.*", ""))\
                          .withColumn(i,F.regexp_replace(F.col(i), r"\s+", ""))\
                          .withColumn(i,F.to_timestamp(F.col(i)))
                          # .withColumn(i, string.strip(F.col(i))) \
             return df      
        

        
        """ definimos el dataframe join"""
        # El dataframe contine las mismas variables definidas del 
        # pivot mas el identificador de registro
        #  ..p_idr..[variables del pivot]..id_etiqueta..
        # el id_etiqueta almacena las llaves de los metodos
        
        re_piv = tim_df( data_piv)
        re_total = tim_df( reg_pobl)
         
        new_jo = spark.createDataFrame([], data_piv.schema)
        new_jo = new_jo.withColumn('p_idr', F.lit(None).cast(StringType()))
        new_jo = new_jo.withColumn('id_etiqueta', F.lit(None).cast(StringType()))
        new_jo = new_jo.select(['p_idr']+data_piv.columns+['id_etiqueta'])
        # 
        
        
        # re_piv = re_piv.select(col("p_fecha_nac")).distinct().withColumn("piv",F.lit(1))
        # re_total = re_total.select(col("p_fecha_nac")).distinct().withColumn("re",F.lit(1))
        # 
        # merge = re_piv.join(re_total,re_piv["p_fecha_nac"] == re_total["p_fecha_nac"],"inner").drop(re_piv["p_fecha_nac"])
        # 
        # abc = re_piv.groupBy('p_fecha_nac').count().toPandas()
        # cde = re_total.groupBy('p_fecha_nac').count().toPandas()
        # 
        # abc.to_csv(r'/home/hadoop/base.txt', index=None, sep=' ', mode='a')
        # cde.to_csv(r'/home/hadoop/reg.txt', index=None, sep=' ', mode='a')
        # 
        # for field in re_piv.schema.fields:
        #   print(field.name +" , "+str(field.dataType))
        # 
        # for field in re_total.schema.fields:
        #   print(field.name +" , "+str(field.dataType))
        # 
        # merge.coalesce(1).write.mode("overwrite").option("header","true").format("csv").save(path_location+'solucion/'+'base.csv')
        # re_total.coalesce(1).write.mode("overwrite").option("header","true").format("csv").save(path_location+'solucion/'+'reg.csv')
        
        
        #opcionborrar=True
        # relizamos un cambio de variable
        
        #axiliar 
        #
        for key in dic_var_det_convi.keys():
                    print(key)
                    #key= "id_cedula_nombre_apell"
                    li= dic_var_det_convi[key]

                    if key== "id_cedulma_iniciale_sexo":
                        re_auxiliar= re_total.selectExpr('*', 'count(*) over (partition \
                                by p_cedula_mad,iniciales, p_sexo ) as cnt').filter(F.col('cnt') >1 ).drop('cnt')
                        re_total= re_total.selectExpr('*', 'count(*) over (partition \
                                by p_cedula_mad,iniciales, p_sexo ) as cnt').filter(F.col('cnt') == 1).drop('cnt')
                    elif  key=="id_cedulpa_iniciale_sexo":
                        re_auxiliar= re_total.selectExpr('*', 'count(*) over (partition \
                                by p_cedula_pad,iniciales, p_sexo ) as cnt').filter(F.col('cnt') >1 ).drop('cnt')
                        re_total= re_total.selectExpr('*', 'count(*) over (partition \
                                by p_cedula_pad, iniciales, p_sexo ) as cnt').filter(F.col('cnt') == 1).drop('cnt')
                    elif  key=="id_cedulma_fna_sexo":
                        re_auxiliar= re_total.selectExpr('*', 'count(*) over (partition \
                                by p_cedula_mad, p_fecha_nac , p_sexo ) as cnt').filter(F.col('cnt') >1 ).drop('cnt')
                        re_total= re_total.selectExpr('*', 'count(*) over (partition \
                                by p_cedula_mad, p_fecha_nac , p_sexo ) as cnt').filter(F.col('cnt') == 1).drop('cnt')
                    elif key=="id_cedulpa_fna_sexo":
                        re_auxiliar= re_total.selectExpr('*', 'count(*) over (partition \
                                by p_cedula_pad,p_fecha_nac ,p_sexo ) as cnt').filter(F.col('cnt') >1 ).drop('cnt')
                        re_total= re_total.selectExpr('*', 'count(*) over (partition \
                                by p_cedula_pad,p_fecha_nac ,p_sexo) as cnt').filter(F.col('cnt') == 1).drop('cnt')
                    
                    elif key=="id_apell_sexo_fnac":
                        re_auxiliar= re_total.selectExpr('*', 'count(*) over (partition \
                                by p_apellidos,p_sexo,p_fecha_nac ) as cnt').filter(F.col('cnt') >1 ).drop('cnt')
                        re_total= re_total.selectExpr('*', 'count(*) over (partition \
                                by p_apellidos,p_sexo,p_fecha_nac) as cnt').filter(F.col('cnt') == 1).drop('cnt')
                    
                
                    elif key=="id_cedulma_apell":
                        re_auxiliar= re_total.selectExpr('*', 'count(*) over (partition \
                                by p_cedula_mad, p_apellidos ) as cnt').filter(F.col('cnt') >1 ).drop('cnt')
                        re_total= re_total.selectExpr('*', 'count(*) over (partition \
                                by p_cedula_mad, p_apellidos) as cnt').filter(F.col('cnt') == 1).drop('cnt')
                                
                    elif key=="id_cedulpa_apell":
                        re_auxiliar= re_total.selectExpr('*', 'count(*) over (partition \
                                by p_cedula_pad, p_apellidos ) as cnt').filter(F.col('cnt') >1 ).drop('cnt')
                        re_total= re_total.selectExpr('*', 'count(*) over (partition \
                                by p_cedula_pad, p_apellidos) as cnt').filter(F.col('cnt') == 1).drop('cnt')
                    
                    
                    else:
                        None

                    if len(li)== 1:
                         dat_to_jog = re_total.join(re_piv, re_total[ li[0] ] == re_piv[li[0] ],"inner")\


                    elif len(li)==2 :
                         dat_to_jog = re_total.join(re_piv, re_total[ li[0] ] == re_piv[li[0] ],"inner")\
                             .where( re_total[ li[1] ] == re_piv[li[1] ])

                    elif len(li)==3:
                         dat_to_jog = re_total.join(re_piv, re_total[ li[0] ] == re_piv[li[0] ],"inner")\
                                .where( (re_total[ li[1] ] == re_piv[li[1] ] )\
                                      &  (re_total[ li[2] ] == re_piv[li[2] ]))

                    elif len(li)==4:
                          dat_to_jog = re_total.join(re_piv, re_total[ li[0] ] == re_piv[li[0] ],"inner")\
                                 .where( (re_total[ li[1] ] == re_piv[li[1] ])\
                                       &  (re_total[ li[2] ] == re_piv[li[2] ])\
                                          &  (re_total[ li[3] ] == re_piv[li[3] ]) )
                    elif len(li)==5:
                         dat_to_jog = re_total.join(re_piv, re_total[ li[0] ] == re_piv[li[0] ],"inner")\
                                .where( (re_total[ li[1] ] == re_piv[li[1] ])\
                                      &  (re_total[ li[2] ] == re_piv[li[2] ])\
                                         &  (re_total[ li[3] ] == re_piv[li[3] ])\
                                             &  (re_total[ li[4] ] == re_piv[li[4] ] ))


                    re_piv = re_piv.subtract(dat_to_jog.select( [ re_piv[i]    for i in re_piv.columns]))
                    new_jo = new_jo.union(dat_to_jog\
                             .select( [re_total[Id_reg]]+ [ re_piv[i]  for i in re_piv.columns])\
                             .withColumn('id_etiqueta', F.lit(key)))
                    re_piv = re_piv.checkpoint()
                    new_jo = new_jo.checkpoint()


        if opcionborrar:
                 re_total = re_total.subtract( re_total.join(new_jo, re_total[ "p_idr" ] == new_jo[ "p_idr" ],"inner")\
                                  .select([ re_total[i] for i in re_total.columns]))



        print("Guardando...")

        #new_jo, re_piv, re_total= checkpoint_write_and_read( new_jo, re_piv, re_total )


        # AB: Del registro civil tomamos nombres y apellidos con un alias:

        reg_nom_ape = re_total.select(col("p_idr"),col("p_nombres").alias("p_nombres_reg"), col("p_apellidos").alias("p_apellidos_reg"),col("p_nombres_apellidos").alias("p_nombres_apellidos_reg"))


        # AB: Unimos a los encontrados, por la cedula recuperada

        new_jo = new_jo.join(reg_nom_ape,['p_idr'],"left")

        # Comentado:::
        
        # AB: Calculo semidifuso
        # AB: No se divide para dos por que al hacer la suma de las ponderaciones,
        # el denominador es igual a 1
        
        cond_nombres_apell = ("p_nombres".upper() in (name.upper() for name in new_jo.columns)) & ("p_apellidos".upper() in (name.upper() for name in new_jo.columns))
        
        if cond_nombres_apell:
          validados = new_jo.withColumn("coef_one",g_funt(new_jo['p_nombres'], new_jo['p_nombres_reg']).alias('coef_one'))\
          .withColumn("coef_two", g_funt(new_jo['p_apellidos'], new_jo['p_apellidos_reg']))\
          .withColumn("coef_nombyamp", g_funt(new_jo['p_nombres_apellidos'], new_jo['p_nombres_apellidos_reg']))\
          .withColumn("tam_nom",F.length(F.col("p_nombres")))\
          .withColumn("tam_nom_reg",F.length(F.col("p_nombres_reg")))\
          .withColumn("tam_apl",F.length(F.col("p_apellidos")))\
          .withColumn("tam_apl_reg",F.length(F.col("p_apellidos_reg")))\
          .withColumn('max_nom', greatest(col('tam_nom'), col('tam_nom_reg')))\
          .withColumn('max_apl', greatest(col('tam_apl'), col('tam_apl_reg')))\
          .withColumn('inv_nom', (F.lit(1)/col('max_nom')))\
          .withColumn('inv_apl', (F.lit(1)/col('max_apl')))\
          .withColumn('denominador', (col('inv_nom') + col('inv_apl')))\
          .withColumn('pon_nom',(col('inv_nom')/col('denominador')))\
          .withColumn('pon_apl',(col('inv_apl')/col('denominador')))\
          .withColumn("prome_nombre_ape", ((F.col("coef_one")*F.col("pon_nom") + F.col('coef_two')*F.col("pon_apl"))))\
          .withColumn("semidifuso", F.greatest( "coef_nombyamp","prome_nombre_ape"))
        else:
          validados = new_jo.withColumn("semidifuso", g_funt(new_jo['p_nombres_apellidos'], new_jo['p_nombres_apellidos_reg']))
        
        validados = validados.where(validados.semidifuso < 0.20 )
        validados =  validados.selectExpr('*',"row_number() OVER (PARTITION BY p_idp ORDER BY semidifuso ASC) as cnt").filter( F.col('cnt') == 1 ).drop('cnt')
        validados = validados.checkpoint()

        validados = validados.select([ validados[i] for i in new_jo.columns])

        # Remoción de los identificados:

        new_jo = new_jo.subtract(validados)

        """construyendo un registro de resultados"""
        match_reg= new_jo.groupBy('id_etiqueta').count().toPandas()
        numdet= match_reg['count'].sum()

        print("Algoritmo deterministcio completado")
       
        ## cambio de variables y transformacion a variables de tiempo
        # re_piv = tim_df( re_piv)
        # re_total = tim_df( reg_pobl)
        # new_jo= tim_df(  new_jo)
        
        

        # new_jo.coalesce(1).write.mode("overwrite").option("header","true").format("csv").save(path_location+'solucion/'+'mach_join_new.csv')
        # re_piv.coalesce(1).write.mode("overwrite").option("header","true").format("csv").save(path_location+'solucion/'+'re_piv_new.csv')
        # validados.coalesce(1).write.mode("overwrite").option("header","true").format("csv").save(path_location+'solucion/'+'validados_new.csv')
        # re_total.coalesce(1).write.mode("overwrite").option("header","true").format("csv").save(path_location+'solucion/'+'re_total_new.csv')
        # new_jo.coalesce(1).write.mode("overwrite").option("header","true").format("csv").save(path_location+'solucion/'+'mach_join_new.csv')

        # AB: No se divide para dos por que al hacer la suma de las ponderaciones,
                                              # el denominador es igual a 1

        for key in dic_var_difu.keys():
           print(key)
           #key='idif_sexo_fna_cedulma_N_A'
           li= dic_var_difu[key]
           if key== 'idif_sexo_fna_N_A':
             
             dat_to_jo = re_total.join(re_piv, re_total[ li[0][0] ] == re_piv[li[0][0] ],"inner")\
             .where(  re_total[ li[0][1] ] == re_piv[li[0][1] ])
             
             if cond_nombres_apell:
               dat_to_jo =dat_to_jo.withColumn("coef_one", g_funt(re_piv[li[1][0]], re_total[li[1][0]]).alias('coef_one'))\
                                   .withColumn("coef_two", g_funt(re_piv[li[1][1]], re_total[li[1][1]]))\
                                   .withColumn("coef_nombyamp", g_funt(re_piv[li[1][2]], re_total[li[1][2]]))\
                                   .withColumn('id_etiqueta', F.lit(key))\
                                   .withColumn("tam_nom",F.length(re_piv[li[1][0]]))\
                                   .withColumn("tam_nom_reg",F.length(re_total[li[1][0]]))\
                                   .withColumn("tam_apl",F.length(re_piv[li[1][1]]))\
                                   .withColumn("tam_apl_reg",F.length(re_total[li[1][1]]))
               dat_to_jo =dat_to_jo.withColumn('max_nom', greatest(col('tam_nom'), col('tam_nom_reg')))\
                                   .withColumn('max_apl', greatest(col('tam_apl'), col('tam_apl_reg')))
               dat_to_jo =dat_to_jo.withColumn('inv_nom', (F.lit(1)/col('max_nom')))\
                                   .withColumn('inv_apl', (F.lit(1)/col('max_apl')))
               dat_to_jo =dat_to_jo.withColumn('denominador', (col('inv_nom') + col('inv_apl')))
               dat_to_jo =dat_to_jo.withColumn('pon_nom',(col('inv_nom')/col('denominador')))\
                                   .withColumn('pon_apl',(col('inv_apl')/col('denominador')))
               dat_to_jo =dat_to_jo.withColumn("prome_nombre_ape", ((F.col("coef_one")*F.col("pon_nom") + F.col('coef_two')*F.col("pon_apl"))))\
                                   .withColumn("Solucion", F.greatest( "coef_nombyamp","prome_nombre_ape"))
             else:
               dat_to_jo =dat_to_jo.withColumn("Solucion", g_funt(re_piv[li[1][2]], re_total[li[1][2]]))\
                                   .withColumn('id_etiqueta', F.lit(key))
               
             axiliar = dat_to_jo.where(dat_to_jo.Solucion<0.20 )
                    
             axiliar =  axiliar.selectExpr('*',"row_number() OVER (PARTITION BY p_idp ORDER BY Solucion ASC) as cnt").filter( F.col('cnt') == 1 ).drop('cnt')
                    
             axiliar = axiliar.join(reg_nom_ape,['p_idr'],"left")

# #
#            # AB: A la solución dejamos los nombres y apellidos del registro
#
             new_jo = new_jo.union(axiliar.select( [re_total[Id_reg]]+ [ re_piv[i]  \
                      for i in re_piv.columns] +[F.col('id_etiqueta')] +[F.col('p_nombres_reg')] +[F.col('p_apellidos_reg')]+[F.col('p_nombres_apellidos_reg')]))
             re_piv = re_piv.subtract(axiliar.select( [ re_piv[i]    for i in re_piv.columns]))
             # new_jo = new_jo.checkpoint()
             #
             # new_jo.coalesce(1).write.mode("overwrite").option("header","true").format("csv").save(path_location+'solucion/'+'axiliar_new.csv')


           #key='idif_sexo_fna_cedulma_N_A_30' and F.col('resto_fecha')>0
           # AB: No se divide para dos por que al hacer la suma de las ponderaciones,
                                   # el denominador es igual a 1
           elif key=='idif_sexo_fna_N_A_30':   # para la ultima parte del algoritmo comparamos a las personas en un rango de 30 dias
               dat_to_jo = re_total.join(re_piv, re_total[ li[0][0] ] == re_piv[li[0][0] ],"inner")\
                             .withColumn( "resto_fecha",  F.abs(F.round(  re_total['p_fecha_nac'].cast("long")-re_piv['p_fecha_nac']\
                             .cast("long")) /(24*3600)) )\
                             .filter( F.col('resto_fecha') < 30 )
                             
               if cond_nombres_apell:
                 axiliar=dat_to_jo.withColumn( "coef_one",    g_funt(re_piv[li[1][0]], re_total[li[1][0]]).alias('coef_one'))\
                                  .withColumn("coef_two", g_funt(re_piv[li[1][1]], re_total[li[1][1]]))\
                                  .withColumn("coef_nombyamp", g_funt(re_piv[li[1][2]], re_total[li[1][2]]))\
                                  .withColumn('id_etiqueta', F.lit(key))\
                                  .withColumn("tam_nom",F.length(re_piv[li[1][0]]))\
                                  .withColumn("tam_nom_reg",F.length(re_total[li[1][0]]))\
                                  .withColumn("tam_apl",F.length(re_piv[li[1][1]]))\
                                  .withColumn("tam_apl_reg",F.length(re_total[li[1][1]])) 
                 axiliar = axiliar.withColumn('max_nom', greatest(col('tam_nom'), col('tam_nom_reg')))\
                                  .withColumn('max_apl', greatest(col('tam_apl'), col('tam_apl_reg')))
                 axiliar = axiliar.withColumn('inv_nom', (F.lit(1)/col('max_nom')))\
                                  .withColumn('inv_apl', (F.lit(1)/col('max_apl')))
                 axiliar = axiliar.withColumn('denominador', (col('inv_nom') + col('inv_apl')))
                 axiliar = axiliar.withColumn('pon_nom',(col('inv_nom')/col('denominador')))\
                                  .withColumn('pon_apl',(col('inv_apl')/col('denominador')))
                 axiliar = axiliar.withColumn("prome_nombre_ape", ((F.col("coef_one")*F.col("pon_nom") + F.col('coef_two')*F.col("pon_apl"))))\
                                   .withColumn("Solucion", F.greatest( "coef_nombyamp","prome_nombre_ape"))
               else:
                 axiliar =dat_to_jo.withColumn("Solucion", g_funt(re_piv[li[1][2]], re_total[li[1][2]]))\
                                   .withColumn('id_etiqueta', F.lit(key))
#
               axiliar = axiliar.where(axiliar.Solucion<0.20 )
               axiliar =  axiliar.selectExpr('*',"row_number() OVER (PARTITION BY p_idp ORDER BY Solucion ASC) as cnt").filter( F.col('cnt') == 1 ).drop('cnt')
               axiliar=axiliar.checkpoint()

               axiliar = axiliar.join(reg_nom_ape,['p_idr'],"left")

               # AB: A la solución dejamos los nombres y apellidos del registro

               new_jo = new_jo.union(axiliar.select( [re_total[Id_reg]]+ [ re_piv[i]  \
                        for i in re_piv.columns] +[F.col('id_etiqueta')] +[F.col('p_nombres_reg')] +[F.col('p_apellidos_reg')]+[F.col('p_nombres_apellidos_reg')]))
               re_piv = re_piv.subtract(axiliar.select( [ re_piv[i]    for i in re_piv.columns]))

               if opcionborrar:
                    re_total = re_total.subtract( unico(axiliar.select( [ re_total[i]    for i in re_total.columns] )))

           else:
               print("No existe sexo o fecha de nacimiento")


        re_piv = re_piv.withColumn('p_idr',re_piv.p_cedula)

        re_piv = re_piv.join(reg_nom_ape,['p_idr'],"left")

        # Difuso pero solo con cedula de los que faltan:
        # Algunos no tienen correcta la fecha, o el sexo pero la ceudula es la correcta
        
        if cond_nombres_apell:
          residuales = re_piv.withColumn("coef_one",g_funt(re_piv['p_nombres'], re_piv['p_nombres_reg']).alias('coef_one'))\
          .withColumn("coef_two", g_funt(re_piv['p_apellidos'], re_piv['p_apellidos_reg']))\
          .withColumn("coef_nombyamp", g_funt(re_piv['p_nombres_apellidos'], re_piv['p_nombres_apellidos_reg']))\
          .withColumn("tam_nom",F.length(F.col("p_nombres")))\
          .withColumn("tam_nom_reg",F.length(F.col("p_nombres_reg")))\
          .withColumn("tam_apl",F.length(F.col("p_apellidos")))\
          .withColumn("tam_apl_reg",F.length(F.col("p_apellidos_reg")))\
          .withColumn('max_nom', greatest(col('tam_nom'), col('tam_nom_reg')))\
          .withColumn('max_apl', greatest(col('tam_apl'), col('tam_apl_reg')))\
          .withColumn('inv_nom', (F.lit(1)/col('max_nom')))\
          .withColumn('inv_apl', (F.lit(1)/col('max_apl')))\
          .withColumn('denominador', (col('inv_nom') + col('inv_apl')))\
          .withColumn('pon_nom',(col('inv_nom')/col('denominador')))\
          .withColumn('pon_apl',(col('inv_apl')/col('denominador')))\
          .withColumn("prome_nombre_ape", ((F.col("coef_one")*F.col("pon_nom") + F.col('coef_two')*F.col("pon_apl"))))\
          .withColumn("semidifuso", F.greatest( "coef_nombyamp","prome_nombre_ape"))
        else:
          residuales = re_piv.withColumn("semidifuso", g_funt(re_piv['p_nombres_apellidos'], re_piv['p_nombres_apellidos_reg']))
        

        cel_val = residuales.where(residuales.semidifuso < 0.2 ).withColumn('id_etiqueta', F.lit("match cedula"))

        cel_val =  cel_val.selectExpr('*',"row_number() OVER (PARTITION BY p_idp ORDER BY semidifuso ASC) as cnt").filter( F.col('cnt') == 1 ).drop('cnt')

        cel_val = cel_val.checkpoint()

        cel_val = cel_val.select([ cel_val[i] for i in re_piv.columns ] + [F.col("id_etiqueta") ]+ [F.col("semidifuso") ] )

        # Remoción de los identificados:

        no_match = residuales.where(residuales.semidifuso >= 0.2 ).withColumn('id_etiqueta', F.lit("no encontrado"))


        no_match =  no_match.selectExpr('*',"row_number() OVER (PARTITION BY p_idp ORDER BY semidifuso ASC) as cnt").filter( F.col('cnt') == 1 ).drop('cnt')

        no_match = no_match.select([ no_match[i] for i in re_piv.columns ] + [F.col("id_etiqueta") ]+ [F.col("semidifuso") ])
        
        # Comentado:::::
        
        print("Empieza el checkpoint")

        #new_jo, re_piv, re_total= final_write_and_read( new_jo, re_piv, re_total )
        re_piv = re_piv.checkpoint()
        new_jo = new_jo.checkpoint()

        """ recolectando datos"""

        print("Fin del checkpoint")

        # new_jo.coalesce(1).write.mode("overwrite").option("header","true").format("csv").save(path_location+'solucion/'+'new_jo.csv')
        # re_total.coalesce(1).write.mode("overwrite").option("header","true").format("csv").save(path_location+'solucion/'+'re_total.csv')
        # re_piv.coalesce(1).write.mode("overwrite").option("header","true").format("csv").save(path_location+'solucion/'+'re_piv.csv')
        
        properties = {
          "user": "automatiza01",
          "password": "STECSDI_2022.auto",
          "driver": "org.postgresql.Driver"
          }

        # AB: Unidos por difuso:

        # new_jo.write.mode("overwrite").option("header","true").format("csv").save(path_location+'solucion/'+'mach_join.csv')
        new_jo.write.jdbc(url="jdbc:postgresql://192.168.29.70:5432/procesamiento?currentSchema=precision", table=name_tlb+"mach_join", mode="overwrite", properties=properties)
        
        print("Guardando los unidos")

        # AB: No unidos:

        # no_match.write.mode("overwrite").option("header","true").format("csv").save(path_location+'solucion/'+'mach_pivot.csv')
        no_match.write.jdbc(url="jdbc:postgresql://192.168.29.70:5432/procesamiento?currentSchema=precision", table=name_tlb+"mach_pivot", mode="overwrite", properties=properties)
        
        print("Guardando los no unidos")

        # AB: Validados:

        # validados.write.mode("overwrite").option("header","true").format("csv").save(path_location+'solucion/'+'validados.csv')
        validados.write.jdbc(url="jdbc:postgresql://192.168.29.70:5432/procesamiento?currentSchema=precision", table=name_tlb+"validados", mode="overwrite", properties=properties)
        
        print("Guardando los validados")
        
        # AB: Residuales (recuperados solo con cedula):

        # cel_val.write.mode("overwrite").option("header","true").format("csv").save(path_location+'solucion/'+'residuales.csv')
        cel_val.write.jdbc(url="jdbc:postgresql://192.168.29.70:5432/procesamiento?currentSchema=precision", table=name_tlb+"residuales", mode="overwrite", properties=properties)
        
        print("Guardando los residuales")
        
        if opcionborrar:
           re_total.write.mode("overwrite").option("header","true").format("csv").save(path_location+'solucion/'+'mach_reg.csv')
        
        # Cierra la sesión de Spark
        spark.stop()

#
#
#
#         """construyendo un registro de resultados"""
#
#
#
#
#
#        # match_reg=new_jo.groupBy('id_etiqueta').count().toPandas()
#        #
#        #
#        # df1= pd.DataFrame([['total deterministico',  numdet]], columns=['id_etiqueta', 'count'])
#        # match_reg=pd.concat( [match_reg, df1])
#        #
#        # numdif= new_jo.count()-numdet
#        # df2= pd.DataFrame([['total difuso',  numdif]], columns=['id_etiqueta', 'count'])
#        # match_reg=pd.concat( [match_reg, df2])
#        #
#        # df4= pd.DataFrame([['total encontrados',  numdet+numdif]], columns=['id_etiqueta', 'count'])
#        # match_reg=pd.concat( [match_reg, df4])
#        #
#        #
#        # df3= pd.DataFrame([['No encontrados',  re_piv.count()]], columns=['id_etiqueta', 'count'])
#        # match_reg=pd.concat( [match_reg, df3])
#        #
#        #
#        # df5= pd.DataFrame([['Validados',  validados.count()]], columns=['id_etiqueta', 'count'])
#        # match_reg=pd.concat( [match_reg, df5])
#        #
#        # print(match_reg)
# 
# 
# 
# 
# #         
#        
#         
# 
# 
# 
# 
# 
# 

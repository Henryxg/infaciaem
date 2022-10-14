
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
from collections import Counter
from math import sqrt
from pyspark import SparkContext
import psycopg2
import pandas as pd
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
""" 
conn = psycopg2.connect(
    database="procesamiento",
    user='abajana',
    password='ab*5t3c5d1.2022',
    host='192.168.29.70',
    port= '5432'
)
  
conn.autocommit = True
cursor = conn.cursor()
  
sql = "SELECT * FROM msp12_aten_consul_ninos_v_04_2022_nor LIMIT 20"
  
  
cursor.execute(sql)
column_names = [desc[0] for desc in cursor.description]
for i in column_names:
    print(i)
conn.commit()
conn.close()
""" 

sc = SparkContext('local[2]', 'test')
spark = SparkSession\
                .builder\
                .appName("PythonWordCount")\
                .getOrCreate()
"""               
reg_pobl = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://192.168.29.70:5432/procesamiento") \
    .option("driver", "org.postgresql.Driver") \
    .option("user", "abajana")\
    .option("password", "ab*5t3c5d1.2022")\
    .option("query", "select * from msp12_aten_consul_ninos_v_04_2022_nor limit 20").load()
    reg_pobl.coalesce(1).write.mode("overwrite").option("header","true").csv( +'mach_join.csv')
"""



field_eq =[ "identificador" , "atemed_id","e_est_salud","e_est_salud_id","e_est_salud_ruc","id_nac_viv",  \
"p_acido_a_s_mayor_12_sem", "p_antitetanica","p_apellidos","p_apellidos_asis","p_apellidos_prof", \
"p_asistido_por","p_bacteriuria","p_calcio_mayor_12_sem","p_cedula","p_cedula_prof","p_cie10_diagnostico",\
"p_clampeo","p_cod_establecimiento","p_consejeria_alim_comp","p_consejeria_lacmatex","p_control_prenat",\
"p_dosis","p_dosis_n","p_dosis_r","p_dosis_vac_1","p_dosis_vac_2","p_dosis_vac_3",\
"p_dosis_vac_4","p_dosis_vac_5","p_drogas","p_ecografia_11_a_13_sem","p_ecografia_menor_20_sem",\
"p_edad","p_edadmeses","p_email_prof","p_embarazo_multiple","p_especialidad_prof",\
"p_esquema","p_esquema_n","p_esquema_r","p_esquema_vac_1","p_esquema_vac_2","p_esquema_vac_3","p_esquema_vac_4",\
"p_esquema_vac_5","p_estado_prestacion_msp","p_estado_registro",\
"p_estado_registro_des","p_exam_emo","p_exam_vdrl","p_exam_vih","p_fe","p_fecha_atenmed_f","p_fecha_atenmed_i","p_fecha_bajapadro_msp",\
"p_fecha_carga","p_fecha_carga_datos_emb","p_fecha_firma","p_fecha_nac",\
"p_fecha_nac_mad","p_fecha_nac_repre","p_fecha_prox_vacuna","p_fecha_registro",\
"p_fecha_ult_toma","p_fecha_vacunacion","p_folatos_indicados",\
"p_fum","p_hb_mayor_igual_20_sem","p_hb_menor_20_sem",\
"p_hierro_mulvit_minpolvo","p_hijos_nacm","p_hijos_vivm","p_hijos_vivos","p_identidad","p_identidad_asis","p_identidad_mad",\
"p_identidad_pad","p_identidad_prof","p_identidad_repre","p_lugar_atenmed","p_lugar_ocur",\
"p_mad_per_lac","p_mayor_igual_20_sem_vih_diag_trat","p_menor_20_sem_vih_diag_trat","p_metodo_deter_sem_ges",\
"p_nacionalidad_mad","p_nom_asis","p_nombres","p_nombres_apellidos","p_nombres_apellidos_mad",\
"p_nombres_apellidos_repre","p_nombres_prof","p_num_ctrl_prenat","p_num_emb","p_num_parto","p_parentesco_repre","p_parr_nac",\
"p_plan_de_parto","p_poblacion_objetivo","p_presc_hierro_acidfol",\
"p_prestacion","p_prod_emb","p_proteinuria","p_recibe_hierro","p_semanas_gest","p_sexo","p_sexo_repre","p_tipo_atenmed",\
"p_tipo_iden","p_tipo_iden_repre","p_tipo_idens_prof","p_tipo_parto","p_tipo_prestacion_salud",\
"p_tipo_seguro","p_vac_dlu24h_rlm_0a5","p_vacuna",\
"p_vacuna_1","p_vacuna_2","p_vacuna_3","p_vacuna_4",\
"p_vacuna_5","p_vacuna_hb","p_vacuna_n","p_vacuna_r","p_vitamina_a"]

""" 
df = pd.read_csv("ms1.csv") 
df1 = pd.DataFrame(columns = list(field_eq))
df1['p_apellidos_prof']=45
df2 = pd.DataFrame(columns = list(field_eq))
"""
db12 =  spark.read.format("csv")\
            .option("header", "true")\
            .load('ms1.csv')


db15 =  spark.read.format("csv")\
            .option("header", "true")\
            .load('ms2.csv')
 #primera parte del algotirmos crear un indicador unic
# el identificacor unico se toma a partir de la formacion de dos fields p_cedula y p_fecha_atenmed_f
def creaides(db1215):
    if  ('p_cedula' in db1215.columns) and ('p_fecha_atenmed_f' in db1215.columns): 
                db1215 = db1215.select('*',F.concat_ws('',\
                            db1215.p_cedula, db1215.p_fecha_atenmed_f ).alias("identificador"))
    return db1215

# creando identificadores
db12 = creaides(db12)
db15 = creaides(db15)
# agragando campos faltantes de los fields dataframe de almacenamiento


def agrega_campo( db1215 , field_eq ):
    camdb1215 = [i.name for i in db1215.schema.fields]
    for i in field_eq:
        if i not in camdb1215:
            db1215 = db1215.withColumn(i, F.lit(None).cast(StringType())) 
    
    return db1215.select(field_eq)

db12=agrega_campo(db12, field_eq)
db15=agrega_campo(db15, field_eq)

db12 = db12.alias('db12')
db15 = db15.alias('db15')


from pyspark import SparkContext
import json
import sys
from pprint import pprint
import statistics
import matplotlib
import matplotlib.pyplot as plt


from matplotlib.pyplot import boxplot

sc = SparkContext()

def mapper_edad(line):
    data = json.loads(line)
    _id = data['_id']
    estacion_salida = data['idunplug_station']
    estacion_llegada = data['idplug_station']
    franja_horaria = data['unplug_hourTime']
    tiempo_viaje = data['travel_time']
    tipo_usuario = data['user_type']
    rango_edad = data['ageRange']
    return rango_edad,tiempo_viaje

def mapper_usuario(line):
    data = json.loads(line)
    _id = data['_id']
    estacion_salida = data['idunplug_station']
    estacion_llegada = data['idplug_station']
    franja_horaria = data['unplug_hourTime']
    tiempo_viaje = data['travel_time']
    tipo_usuario = data['user_type']
    rango_edad = data['ageRange']
    return tipo_usuario,tiempo_viaje

def mapper_usuario_unico(line):
    data = json.loads(line)
    _id = data['_id']
    estacion_salida = data['idunplug_station']
    estacion_llegada = data['idplug_station']
    franja_horaria = data['unplug_hourTime']
    tiempo_viaje = data['travel_time']
    tipo_usuario = data['user_type']
    rango_edad = data['ageRange']
    return _id,1

def crear_lista_rdd(lista):
    result1 = []
    result2 = []
    for i in lista:
        result1.append(i[0])
        result2.append(list(i[1]))
    return [result1,result2]

def crear_lista(lista):
    result1 = []
    result2 = []
    for i in lista:
        result1.append(i[0])
        result2.append(i[1])
    return [result1,result2]

def estudio_edad(rdd_base,archivo_salida):
	#Filtramos por edad, y nos quedamos con tupla (edad,list tiempo), despues calculamos media
	rdd_edad_media = rdd_base.map(mapper_edad).groupByKey().map(lambda x : (x[0],sum(list(x[1]))/len(list(x[1])))).collect()
	archivo_salida.write(str(rdd_edad_media))
	ejes = crear_lista(list(rdd_edad_media))
	matplotlib.pyplot.bar(ejes[0],ejes[1])
	matplotlib.pyplot.ylabel('Media')
	matplotlib.pyplot.xlabel('Edad')
	plt.title('Tiempo medio dependiendo de la edad')
	matplotlib.pyplot.show()

def estudio_usuario(rdd_base,archivo_salida):
	#Filtramos por usuario, y nos quedamos con tupla (usuario,list tiempo), despues calculamos media
	rdd_usuario_media = rdd_base.map(mapper_usuario).groupByKey().map(lambda x : (x[0],sum(list(x[1]))/len(list(x[1])))).collect()
	archivo_salida.write(str(rdd_usuario_media))
	ejes = crear_lista(list(rdd_usuario_media))
	matplotlib.pyplot.bar(ejes[0],ejes[1])
	matplotlib.pyplot.ylabel('Media')
	matplotlib.pyplot.xlabel('Usuario')
	plt.title('Tiempo medio dependiendo del usuario')
	matplotlib.pyplot.show()
    

def estudio_usuario_unico(lrdd,archivo_salida):
    usuarios = []
    mes = []
    for i in range(6):
        rdd_usuario_unico = lrdd[i].map(mapper_usuario_unico).groupByKey().map(lambda x : (x[0],1)).collect()
        archivo_salida.write(str(rdd_usuario_unico))
        usuario = sum(crear_lista(list(rdd_usuario_unico))[1])
        usuarios.append(usuario)
        mes.append(i)
    matplotlib.pyplot.bar(mes,usuarios)
    matplotlib.pyplot.ylabel('Usuarios Unicos')
    matplotlib.pyplot.xlabel('Mes')
    plt.title('Tiempo medio dependi')
    plt.show()
    
def proceso(rdd,archivo_salida,lrdd):
    #estudio_usuario(rdd,archivo_salida)
    #estudio_edad(rdd,archivo_salida)
    estudio_usuario_unico(lrdd,archivo_salida)

def main(sc, years, months):
    rdd = sc.parallelize([])
    lrdd = []
    for y in years:
        for m in months:
            if m<10:
                lrdd.append(sc.textFile(f"{y}0{m}_movements.json"))
            else:
                lrdd.append(sc.textFile(f"{y}{m}_movements.json"))
    for y in years:
        for m in months:
            if m<10:
                filename = f"{y}0{m}_movements.json"
            else:
                filename = f"{y}{m}_movements.json"
            rdd=rdd.union(sc.textFile(filename))
    archivo_salida=open('bicimad.txt','w')
    proceso(rdd,archivo_salida,lrdd)
    archivo_salida.close()

#Python3 bicimad.py (habra que revisar esto)
if __name__ =="__main__":
	if len(sys.argv) <= 1:
		years=[2018]
	else:
		years=list(map(int, sys.argv[1][1:-1].split(",")))
	if len(sys.argv) <= 2:
		months=[6]
	else:
		months=list(map(int, sys.argv[2][1:-1].split(",")))

	main(sc, years,months)
    







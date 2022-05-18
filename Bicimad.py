from pyspark import SparkContext
import json
import sys
from pprint import pprint
import statistics
import matplotlib
import matplotlib.pyplot as plt


#import matplotlib
#matplotlib.use('TkAgg')
#import matplotlib.pyplot as plt
from matplotlib.pyplot import boxplot

sc = SparkContext()


def mapper(line):
    data = json.loads(line)
    _id = data['_id']
    estacion_salida = data['idunplug_station']
    estacion_llegada = data['idplug_station']
    franja_horaria = data['unplug_hourTime']
    tiempo_viaje = data['travel_time']
    tipo_usuario = data['user_type']
    rango_edad = data['ageRange']
    return _id, estacion_salida, estacion_llegada, franja_horaria, tiempo_viaje,tipo_usuario,rango_edad

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


def mapper_hora(line):
    data = json.loads(line)
    franja_horaria = data['unplug_hourTime'].split('T')[1][:-1]
    tiempo_viaje = data['travel_time']
    return franja_horaria,tiempo_viaje    

def mapper_distrito(line):
    data = json.loads(line)
    estacion_salida = data['idunplug_station']
    estacion_llegada = data['idplug_station']
    franja_horaria = data['unplug_hourTime']
    tiempo_viaje = data['travel_time']
    tipo_usuario = data['user_type']
    rango_edad = data['ageRange']
    return estacion_salida,tiempo_viaje

def asociar_distrito(tupla):
    if tupla[0] <= 63 and tupla[0] >= 1:
        nodo = 'Centro'
    elif (tupla[0] >= 116 and tupla[0] <= 126) or tupla[0] == 132:
        nodo = 'Moncloa'
    elif tupla[0] >= 139 and tupla[0] <= 143:
        nodo = 'Tetuán'
    elif tupla[0] >= 156 and tupla[0] <= 161:
        nodo = 'Chamberí'
    elif (tupla[0] >= 144 and tupla[0] <= 155) or (tupla[0] >= 168 and tupla[0] <= 173) or (tupla[0]== 137) or (tupla[0]==138):
        nodo = 'Chamartin'
    elif (tupla[0] >= 92 and tupla[0] <= 115) or (tupla[0] >= 162 and tupla[0] <= 167):
        nodo = 'Salamanca'
    elif (tupla[0] >= 64 and tupla[0] <= 91):
        nodo = 'Retiro'
    else:
        nodo = 'Arganzuela'
    return nodo,tupla[1]

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


def estudio_usuario(rdd_base,archivo_salida):
	#Filtramos por usuarios, y nos quedamos con tupla (usuario,list tiempo), despues calculamos media
	rdd_usuario_media = rdd_base.map(mapper_usuario).groupByKey().map(lambda x : (x[0],sum(list(x[1]))/len(list(x[1])))).collect()
	archivo_salida.write(str(rdd_usuario_media))

	rdd_usuario_mediana = rdd_base.map(mapper_usuario).groupByKey().map(lambda x : (x[0],statistics.median(list(x[1])))).collect()
	archivo_salida.write(str(rdd_usuario_mediana))

	#Boxplot usuario mediana
	data_usuario = rdd_base.map(mapper_usuario).groupByKey().collect()
	r = crear_lista_rdd(list(data_usuario))
	matplotlib.pyplot.boxplot(x=r[1])
	grafico = matplotlib.pyplot.xticks(list(range(1,len(r[0])+1)),r[0])
	plt.title('Boxlplot usuario')
	plt.show()

	ejes = crear_lista(list(rdd_usuario_media))
	matplotlib.pyplot.bar(ejes[0],ejes[1])
	matplotlib.pyplot.ylabel('Media')
	matplotlib.pyplot.xlabel('Usuario')
	plt.title('Tiempo medio dependiendo del usuario')
	matplotlib.pyplot.show()

	ejes = crear_lista(list(rdd_usuario_mediana))
	matplotlib.pyplot.bar(ejes[0],ejes[1])
	matplotlib.pyplot.ylabel('Mediana')
	matplotlib.pyplot.xlabel('Usuario')
	matplotlib.pyplot.show()

def estudio_edad(rdd_base,archivo_salida):
	#Filtramos por edad, y nos quedamos con tupla (edad,list tiempo), despues calculamos media
	rdd_edad_media = rdd_base.map(mapper_edad).groupByKey().map(lambda x : (x[0],sum(list(x[1]))/len(list(x[1])))).collect()
	archivo_salida.write(str(rdd_edad_media))


	rdd_edad_mediana = rdd_base.map(mapper_edad).groupByKey().map(lambda x: (x[0],statistics.median(list(x[1])))).collect()
	archivo_salida.write(str(rdd_edad_mediana))

	#Boxplot edad mediana
	data_edad = rdd_base.map(mapper_edad).groupByKey().collect()
	r = crear_lista_rdd(list(data_edad))
	matplotlib.pyplot.boxplot(x=r[1])
	grafico = matplotlib.pyplot.xticks(list(range(1,len(r[0])+1)),r[0])
	plt.show()

	ejes = crear_lista(list(rdd_edad_media))
	matplotlib.pyplot.bar(ejes[0],ejes[1])
	matplotlib.pyplot.ylabel('Media')
	matplotlib.pyplot.xlabel('Edad')
	matplotlib.pyplot.show()

	ejes = crear_lista(list(rdd_edad_mediana))
	matplotlib.pyplot.bar(ejes[0],ejes[1])
	matplotlib.pyplot.ylabel('Mediana')
	matplotlib.pyplot.xlabel('Edad')
	matplotlib.pyplot.show()


def estudio_distrito(rdd_base,archivo_salida):
	#Filtramos por distrito y nos quedamos con tupla (franja, list tiempo)
	rdd_distrito_media = rdd_base.map(mapper_distrito).map(asociar_distrito).groupByKey().map(lambda x : (x[0],sum(list(x[1]))/len(list(x[1])))).collect()
	archivo_salida.write(str(rdd_distrito_media))


	rdd_distrito_mediana = rdd_base.map(mapper_distrito).map(asociar_distrito).groupByKey().map(lambda x: (x[0],statistics.median(list(x[1])))).collect()
	archivo_salida.write(str(rdd_distrito_mediana))


	#Boxplot distrito mediana
	data_distrito = rdd_base.map(mapper_distrito).map(asociar_distrito).groupByKey().collect()
	r = crear_lista_rdd(list(data_distrito))
	matplotlib.pyplot.boxplot(x=r[1])
	ax = matplotlib.pyplot.xticks(list(range(1,len(r[0])+1)),r[0])
	plt.show()

	ejes = crear_lista(list(rdd_distrito_media))
	matplotlib.pyplot.bar(ejes[0],ejes[1])
	matplotlib.pyplot.ylabel('Media')
	matplotlib.pyplot.xlabel('Distrito')
	matplotlib.pyplot.show()

	ejes = crear_lista(list(rdd_distrito_mediana))
	matplotlib.pyplot.bar(ejes[0],ejes[1])
	matplotlib.pyplot.ylabel('Mediana')
	matplotlib.pyplot.xlabel('Distrito')
	matplotlib.pyplot.show()

def estudio_horario(rdd_base,archivo_salida):
	#Filtramos por franja horario y nos quedamos con tupla (franja, list tiempo)
	rdd_horario_media = rdd_base.map(mapper_hora).groupByKey().map(lambda x : (x[0],sum(list(x[1]))/len(list(x[1])))).collect()
	archivo_salida.write(str(rdd_horario_media))


	rdd_horario_mediana = rdd_base.map(mapper_hora).groupByKey().map(lambda x:(x[0],statistics.median(list(x[1])))).collect()
	archivo_salida.write(str(rdd_horario_mediana))


	#Boxplot horario mediana
	data_horario = rdd_base.map(mapper_hora).groupByKey().collect()
	r = crear_lista_rdd(list(data_horario))
	ejes_x = list(map((lambda x: x[:5]),r[0]))
	matplotlib.pyplot.boxplot(x=r[1])
	grafico = matplotlib.pyplot.xticks(list(range(1,len(ejes_x)+1)),ejes_x)
	plt.show()

	ejes = crear_lista(list(rdd_horario_media))
	ejes_x = list(map((lambda x: x[:5]),ejes[0]))
	matplotlib.pyplot.bar(ejes_x,ejes[1])
	matplotlib.pyplot.ylabel('Media')
	matplotlib.pyplot.xlabel('Horarios')
	matplotlib.pyplot.show()

	ejes = crear_lista(list(rdd_horario_mediana))
	ejes_x = list(map((lambda x: x[:5]),ejes[0]))
	matplotlib.pyplot.bar(ejes_x,ejes[1])
	matplotlib.pyplot.ylabel('Mediana')
	matplotlib.pyplot.xlabel('Horarios')
	matplotlib.pyplot.show()

def proceso(rdd,archivo_salida):
	#estudio_usuario(rdd,archivo_salida)
	estudio_edad(rdd,archivo_salida)
	#estudio_distrito(rdd,archivo_salida)
	#estudio_horario(rdd,archivo_salida)

	

def main(sc, years, months):
	rdd = sc.parallelize([])
	for y in years:
		for m in months:
			if m<10:
				filename = f"{y}0{m}_movements.json"
			else:
				filename = f"{y}{m}_movements.json"
			rdd=rdd.union(sc.textFile(filename))
	archivo_salida=open('bicimad.txt','w')
	proceso(rdd,archivo_salida)
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
    







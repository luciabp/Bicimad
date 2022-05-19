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
    tiempo_viaje = data['travel_time']
    rango_edad = data['ageRange']
    return rango_edad,tiempo_viaje
"""
def mapper_usuario(line):
    data = json.loads(line)
    tiempo_viaje = data['travel_time']
    tipo_usuario = data['user_type']
    return tipo_usuario,tiempo_viaje
"""
def mapper_usuario_unico(line):
    data = json.loads(line)
    codigo_usuario = data['user_day_code']
    return codigo_usuario,1

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

def estudio_edad(rdd19, rdd20, archivo_salida):
	#Filtramos por edad, y nos quedamos con tupla (edad,list tiempo), despues calculamos media
	rdd19_edad_media = rdd19.map(mapper_edad).groupByKey().map(lambda x : (x[0],sum(list(x[1]))/len(list(x[1])))).collect()
	archivo_salida.write(str(rdd19_edad_media))
	ejes19 = crear_lista(list(rdd19_edad_media))
	rdd20_edad_media = rdd20.map(mapper_edad).groupByKey().map(lambda x : (x[0],sum(list(x[1]))/len(list(x[1])))).collect()
	archivo_salida.write(str(rdd20_edad_media))
	ejes20 = crear_lista(list(rdd20_edad_media))
    # GRAFICO
	bar_width = 0.3
	ejes20_2 = []
	for i in ejes20[0]:
		ejes20_2.append(i+bar_width)
        
	matplotlib.pyplot.bar(ejes19[0],ejes19[1],bar_width,color='b',label='2019')
	matplotlib.pyplot.bar(ejes20_2,ejes20[1],bar_width,color='r',label='2020')
	plt.legend(loc='best')
	plt.title('Tiempo medio dependiendo de la edad')
	plt.show()
    # GRAFICO
	# matplotlib.pyplot.bar(ejes[0],ejes[1])
	# matplotlib.pyplot.ylabel('Media')
	# matplotlib.pyplot.xlabel('Edad')
	# plt.title('Tiempo medio dependiendo de la edad')
	# matplotlib.pyplot.show()
"""
def estudio_usuario(rdd19,rdd20,archivo_salida):
	#Filtramos por usuario, y nos quedamos con tupla (usuario,list tiempo), despues calculamos media
	rdd19_usuario_media = rdd19.map(mapper_usuario).groupByKey().map(lambda x : (x[0],sum(list(x[1]))/len(list(x[1])))).collect()
	archivo_salida.write(str(rdd19_usuario_media))
	ejes19 = crear_lista(list(rdd19_usuario_media))
    
	rdd20_usuario_media = rdd20.map(mapper_usuario).groupByKey().map(lambda x : (x[0],sum(list(x[1]))/len(list(x[1])))).collect()
	archivo_salida.write(str(rdd20_usuario_media))
	ejes20  = crear_lista(list(rdd20_usuario_media))
    # GRAFICO
	bar_width = 0.2
	ejes20_2 = []
	for i in ejes20[0]:
		ejes20_2.append(i+bar_width)
	print(ejes19[0])
	print(ejes20_2)
	
	plt.hist([ejes19[1], ejes20[1]], ejes19[0],label=['2019', '2020'])
	plt.legend(loc='upper left')
	plt.show()
    
	matplotlib.pyplot.bar(ejes19[0],ejes19[1],bar_width,color='b',label='2019')
	matplotlib.pyplot.bar(ejes20_2,ejes20[1],bar_width,color='r',label='2020')
	plt.legend(loc='best')
	plt.title('Tiempo medio dependiendo del usuario')
	plt.show()
    # grafico
	#matplotlib.pyplot.bar(ejes[0],ejes[1])
	#matplotlib.pyplot.ylabel('Media')
	#matplotlib.pyplot.xlabel('Usuario')
	#plt.title('Tiempo medio dependiendo del usuario')
	#matplotlib.pyplot.show()
    
"""
def estudio_usuario_unico(lrdd19, lrdd20, months,archivo_salida):
    usuarios19 = []
    usuarios20 = []
    for i in range(len(months)):
        rdd19_usuario_unico = (lrdd19[i]).map(mapper_usuario_unico).groupByKey().map(lambda x : (x[0],1)).collect()
        archivo_salida.write(str(rdd19_usuario_unico))
        usuario19 = sum(crear_lista(list(rdd19_usuario_unico))[1])
        usuarios19.append(usuario19)
        rdd20_usuario_unico = (lrdd20[i]).map(mapper_usuario_unico).groupByKey().map(lambda x : (x[0],1)).collect()
        archivo_salida.write(str(rdd20_usuario_unico))
        usuario20 = sum(crear_lista(list(rdd20_usuario_unico))[1])
        usuarios20.append(usuario20)
    # GRAFICO
    bar_width = 0.3
    mes2 = []
    for i in months:
        mes2.append(i+bar_width)
    matplotlib.pyplot.bar(months,usuarios19,bar_width,color='b',label='2019')
    matplotlib.pyplot.bar(mes2,usuarios20,bar_width,color='r',label='2020')
    plt.legend(loc='best')
    plt.title('Número de usuarios únicos')
    #plt.hist([usuarios19, usuarios20], months, label=['2019', '2020'])
    #plt.legend(loc='upper left')
    plt.show()
    #matplotlib.pyplot.bar(meses, usuarios)
    #matplotlib.pyplot.ylabel('Usuarios Unicos')
    #matplotlib.pyplot.xlabel('Mes')
    #plt.title('Tiempo medio dependi')
    #plt.show()
    
def proceso(rdd19,rdd20,lrdd19,lrdd20, months, archivo_salida):
    #estudio_usuario(rdd19, rdd20,archivo_salida)
    estudio_edad(rdd19, rdd20, archivo_salida)
    estudio_usuario_unico(lrdd19, lrdd20, months, archivo_salida)

def main(sc, years, months):
    rdd19 = sc.parallelize([])
    rdd20 = sc.parallelize([])
    lrdd19 = []
    lrdd20 = []
    for y in years:
        for m in months:
            if y == 2020 or (y == 2019 and m > 6):
                if m<10:
                    filename = f"{y}0{m}_movements.json"
                else:
                    filename = f"{y}{m}_movements.json"
            else:
                filename = f"{y}0{m}_Usage_Bicimad.json"
            if y == 2019:
                rdd19=rdd19.union(sc.textFile(filename))
                lrdd19.append(sc.textFile(filename))
            else:
                rdd20 = rdd20.union(sc.textFile(filename))
                lrdd20.append(sc.textFile(filename))
    archivo_salida=open('bicimad.txt','w')
    proceso(rdd19,rdd20,lrdd19,lrdd20, months, archivo_salida)
    archivo_salida.close()

#Python3 bicimad.py (habra que revisar esto)
if __name__ =="__main__":
	if len(sys.argv) <= 1:
		years=[2019,2020]
	else:
		years=list(map(int, sys.argv[1][1:-1].split(",")))
	if len(sys.argv) <= 2:
		months=[5,6,7,8,9,10]
	else:
		months=list(map(int, sys.argv[2][1:-1].split(",")))

	main(sc, years,months)


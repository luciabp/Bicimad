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

def mapper_usuario(line):
    data = json.loads(line)
    tiempo_viaje = data['travel_time']
    tipo_usuario = data['user_type']
    return tipo_usuario,tiempo_viaje

def mapper_plugstation(line):
    data = json.loads(line)
    plug = data['idplug_base']
    return plug,1

def mapper_unplugstation(line):
    data = json.loads(line)
    unplug = data['idunplug_base']
    return unplug,1

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

#-------------------------------------------------------------------------------------------------

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
    
#----------------------------------------------------------------------------------------------------

def asociar_barrio(tupla):
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
    
def estudio_station(rdd_base,archivo_salida):
    
    rdd_plugstation = rdd_base.map(mapper_plugstation).map(asociar_barrio).groupByKey().map(lambda x : (x[0],sum(list(x[1])))).collect()
    #rdd_unplugstation = rdd_base.map(mapper_unplugstation).map(asociar_barrio).groupByKey().map(lambda x : (x[0],sum(list(x[1])))).collect()
    archivo_salida.write(str(rdd_plugstation))
    #archivo_salida.write(str(rdd_unplugstation))
    ejes_plug = crear_lista(list(rdd_plugstation))
    #ejes_unplug = crear_lista(list(rdd_unplugstation))
    fig, ax =plt.subplots(1,1)
    data = []
    for i in range(len(ejes_plug[1])): 
        data.append([ejes_plug[1][i]])#,ejes_unplug[1][i]])
    column_labels=["Plug Station", "Unplug Station"]
    ax.axis('tight')
    ax.axis('off')
    ax.table(cellText=data,rowLabels=ejes_plug[0],colLabels=column_labels,loc="center")

    plt.show()


#----------------------------------------------------------------------------------------------------------


def estudio_usuario_unico(lrdd,archivo_salida):
    usuarios = []
    mes = []
    for i in range(6):
        rdd_usuario_unico = (lrdd[i]).map(mapper_usuario_unico).groupByKey().map(lambda x : (x[0],1)).collect()
        archivo_salida.write(str(rdd_usuario_unico))
        usuario = sum(crear_lista(list(rdd_usuario_unico))[1])
        usuarios.append(usuario)
        mes.append(i+1)
    matplotlib.pyplot.bar(mes,usuarios)
    matplotlib.pyplot.ylabel('Usuarios Unicos')
    matplotlib.pyplot.xlabel('Mes')
    plt.title('Tiempo medio dependi')
    plt.show()
    
def proceso(rdd,archivo_salida,lrdd):
    #estudio_usuario(rdd,archivo_salida)
    #estudio_edad(rdd,archivo_salida)
    #estudio_usuario_unico(lrdd,archivo_salida)
    estudio_station(rdd, archivo_salida)

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
		months=[1,2,3,4,5,6]
	else:
		months=list(map(int, sys.argv[2][1:-1].split(",")))

	main(sc, years,months)
    







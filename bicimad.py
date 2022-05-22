
from pyspark import SparkContext
import json
import sys
import matplotlib
import matplotlib.pyplot as plt

#-------------------------------------------------------------------------------------------------

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
    plug = data['idplug_station']
    return plug,1

def mapper_unplugstation(line):
    data = json.loads(line)
    unplug = data['idunplug_station']
    return unplug,1

def mapper_usuario_unico(line):
    data = json.loads(line)
    codigo_usuario = data['user_day_code']
    return codigo_usuario,1

#-------------------------------------------------------------------------------------------------
    
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

#Los distintos distritos con sus barrios 
#En cada barrio tenemos una lista de los distintos codigos de Bicimad que hay
    
#Distrito 1 - Centro
Palacio = [9,23,24,30,35,36,37,38,39,166]
Embajadores = [40,41,42,43,44,45,48,49,50,51,53]
Cortes = [27,28,29,34,52,67,86]
Justicia = [5,6,7,10,18,19,20,26,54,58]
Universidad =[2,4,11,12,13,15,16,17,55,57,59,211]
Sol = [1,22,25,31,32,33,56,210,64]

#Distrito 2 - Arganzuela
Imperial = [167,236]
Acacias = [46,47,162,163,169,170,173,174]
Chopera = [165,175]
Legazpi = [235]
Delicias = [164,171,176,177]
Palos_de_Moguer = [118,120,121,168,172]
Atocha = [80,119,180]

#Distrito 3 - Retiro
Pacifico = [77,79,178]
Adelfas = [78,84]
Estrella = [73,84,182,82]
Ibiza = [61,62,63,181]
Jeronimos = [60,65,68,69,70,74,75,81,85]
Niño_Jesus = [71,72,76,83]

#Distrito 4 - Salamanca
Recoletos = [87,90,92,93,94,95,96,102,104,105,106,107]
Goya = [89,91,97,98,99,100,101,108,183]
Fuente_del_Ebro = [184,185,187,66]
Guindalera = [191,192,193,194,240,241,242,243,21]
Lista = [143,144,145,186]
Castellana = [88,103,109,142,190]

#Distrito 5 - Chamartin
El_Viso = [136,137,139,140,146,147,148,149,150,197,250]
Prosperidad = [195,251]
Ciudad_Jardin = [196]
Hispanoamerica = [158,159,160,253]
Nueva_españa = [157,209,254,255]
Castilla = [208,249,252]

#Distrito 6 - Tetuan
Bellas_vistas = [205,237]
Cuatro_Caminos = [133,151,152,153,154,155,238]
Castillejos = [156]
Valdeacederas = [207]
Berruguete = [206,239]

#Distrito 7 - Chamberi
Gaztambide = [239,111,123,128,199]
Arapiles = [3,127,200,245]
Trafalgar = [124,126,246]
Almagro = [8,122,125,141,198]
Rios_Rosas = [129,130,131,132,134,135,138,201,247]
Vallehermoso = [202,203,244]

#Distrito 8 - Fuencarral 
La_Paz = [217,248]

#Distrito 9 - Moncloa
Casa_de_Campo = [110,113,161]
Arguelles = [14,112,114,115,116,117]
Ciudad_Universitaria = [256,257,258,259,260,261]

#Distrito 10 - Latina
Carmenes = [223]
Puerta_del_Angel = [216,224]

#Distrito 11 - Carabanchel
Comilla = [228]
San_Isidro = [212]

#Distrito 12 - Usera
Moscardo = [213,222]

#Distrito 13 - Puente de Vallecas
San_Diego = [179,227]
Numancia = [225,226]

#Distrito 14 - Moratalaz
Pavones = [221]
Marroquina = [219]
Media_Legua = [218,220]

#Distrito 15 - Ciudad Lineal
Ventas = [188,232,233,234]
Pueblo_Nuevo = [215]
Concepcion = [189,229]
San_Pascual = [214,230,231]

Barrios = {'nombres' : ['Palacio','Embajadores','Cortes','Justicia','Universidad','Sol',
                        'Imperial','Acacias','Chopera','Legazpi','Delicias','Palos_de_Moguer','Atocha',
                        'Pacifico','Adelfas','Estrella','Ibiza','Jeronimos','Niño_Jesus',
                        'Recoletos','Goya','Fuente_del_Ebro','Guindalera','Lista','Castellana',
                        'El_Viso','Prosperidad','Ciudad_Jardin','Hispanoamerica','Nueva_españa','Castilla',
                        'Bellas_vistas','Cuatro_Caminos','Castillejos','Valdeacederas','Berruguete',
                        'Gaztambide','Arapiles','Trafalgar','Almagro','Rios_Rosas','Vallehermoso',
                        'La_Paz',
                        'Casa_de_Campo','Arguelles','Ciudad_Universitaria',
                        'Carmenes','Puerta_del_Angel',
                        'Comilla','San_Isidro',
                        'Moscardo',
                        'San_Diego','Numancia',
                        'Pavones','Marroquina','Media_Legua',
                        'Ventas','Pueblo_Nuevo','Concepcion','San_Pascual'] , 
              'lista' : [Palacio,Embajadores,Cortes,Justicia,Universidad,Sol,
                         Imperial,Acacias,Chopera,Legazpi,Delicias,Palos_de_Moguer,Atocha,
                         Pacifico,Adelfas,Estrella,Ibiza,Jeronimos,Niño_Jesus,
                         Recoletos,Goya,Fuente_del_Ebro,Guindalera,Lista,Castellana,
                         El_Viso,Prosperidad,Ciudad_Jardin,Hispanoamerica,Nueva_españa,Castilla,
                         Bellas_vistas,Cuatro_Caminos,Castillejos,Valdeacederas,Berruguete,
                         Gaztambide,Arapiles,Trafalgar,Almagro,Rios_Rosas,Vallehermoso,
                         La_Paz,
                         Casa_de_Campo,Arguelles,Ciudad_Universitaria,
                         Carmenes,Puerta_del_Angel,
                         Comilla,San_Isidro,
                         Moscardo,
                         San_Diego,Numancia,
                         Pavones,Marroquina,Media_Legua,
                         Ventas,Pueblo_Nuevo,Concepcion,San_Pascual]}

def asociar_barrio(tupla):
    for i in range(len(Barrios['lista'])):
        if tupla[0] in Barrios['lista'][i]:
            nodo = Barrios['nombres'][i]
            return nodo,tupla[1]
    return"nada",1
    
#----------------------------------------------------------------------------------------------------
    #Funcion para obtener un estudio del tiempo que invierten los usuarios en la bici en funcion de su edad
def estudio_edad(rdd19, rdd20, archivo_salida):
    
    #Datos 2019
	rdd19_edad_media = rdd19.map(mapper_edad).groupByKey().map(lambda x : (x[0],sum(list(x[1]))/len(list(x[1])))).collect()
	archivo_salida.write(str(rdd19_edad_media))
	ejes19 = crear_lista(list(rdd19_edad_media))
    
    #Datos 2020
	rdd20_edad_media = rdd20.map(mapper_edad).groupByKey().map(lambda x : (x[0],sum(list(x[1]))/len(list(x[1])))).collect()
	archivo_salida.write(str(rdd20_edad_media))
	ejes20 = crear_lista(list(rdd20_edad_media))
    
    # Representamos el grafico de barras
	bar_width = 0.3 #ancho de las barras
    
    #Para poner las barras una detrás de la otra
	for i in ejes20[0]:
		ejes20[0][i] = ejes20[0][i] + bar_width

    #Buscamos tener el eje Y en minutos
	for i in range(0,len(ejes19[1])):
		ejes19[1][i] = ejes19[1][i]/60
		ejes20[1][i] = ejes20[1][i]/60
        
	matplotlib.pyplot.bar(ejes19[0],ejes19[1],bar_width,color='b',label='2019')
	matplotlib.pyplot.bar(ejes20[0],ejes20[1],bar_width,color='r',label='2020')
	plt.legend(loc='best')
	plt.title('Tiempo medio dependiendo de la edad')
	plt.show()

#-------------------------------------------------------------------------------------------------
     #Funcion para obtener un estudio del numero de usuarios únicos
def estudio_usuario_unico(lrdd19, lrdd20, months,archivo_salida):
    
    usuarios19 = []
    usuarios20 = []
    
    for i in range(len(months)):
        #2019
        rdd19_usuario_unico = (lrdd19[i]).map(mapper_usuario_unico).groupByKey().collect()
        archivo_salida.write(str(rdd19_usuario_unico))
        usuario19 = len((list(rdd19_usuario_unico)))
        usuarios19.append(usuario19)
        #2020
        rdd20_usuario_unico = (lrdd20[i]).map(mapper_usuario_unico).groupByKey().collect()
        archivo_salida.write(str(rdd20_usuario_unico))
        usuario20 = len((list(rdd20_usuario_unico)))
        usuarios20.append(usuario20)
        
    # Representamos el grafico de barras
    bar_width = 0.3 #ancho de las barras
    
    #Para poner las barras una detrás de la otra
    mes2 = []
    for i in months:
        mes2.append(i+bar_width)
        
    matplotlib.pyplot.bar(months,usuarios19,bar_width,color='b',label='2019')
    matplotlib.pyplot.bar(mes2,usuarios20,bar_width,color='r',label='2020')
    plt.legend(loc='best')
    plt.title('Número de usuarios únicos')
    plt.show()
    
#--------------------------------------------------------------------------------------------

def estudio_station(rdd19,rdd20,archivo_salida):
    
    rdd_unplugstation19 = rdd19.map(mapper_unplugstation).map(asociar_barrio).sortByKey(True,1).groupByKey().map(lambda x : (x[0],sum(list(x[1])))).filter(lambda x : x[0]!="nada").sortBy(lambda x:-x[1]).collect()
    rdd_unplugstation20 = rdd20.map(mapper_unplugstation).map(asociar_barrio).sortByKey(True,1).groupByKey().map(lambda x : (x[0],sum(list(x[1])))).filter(lambda x : x[0]!="nada").sortBy(lambda x:-x[1]).collect()
    rdd_plugstation19 = rdd19.map(mapper_plugstation).map(asociar_barrio).sortByKey(True,1).groupByKey().map(lambda x : (x[0],sum(list(x[1])))).filter(lambda x : x[0]!="nada").sortBy(lambda x:-x[1]).collect()
    rdd_plugstation20 = rdd20.map(mapper_plugstation).map(asociar_barrio).sortByKey(True,1).groupByKey().map(lambda x : (x[0],sum(list(x[1])))).filter(lambda x : x[0]!="nada").sortBy(lambda x:-x[1]).collect()
    
    archivo_salida.write(str(rdd_unplugstation19))
    archivo_salida.write(str(rdd_unplugstation20))
    archivo_salida.write(str(rdd_plugstation19))
    archivo_salida.write(str(rdd_plugstation20))
    
    ejes_unplug19 = crear_lista(list(rdd_unplugstation19))
    ejes_unplug20 = crear_lista(list(rdd_unplugstation20))
    ejes_plug19 = crear_lista(list(rdd_plugstation19))
    ejes_plug20 = crear_lista(list(rdd_plugstation20))

    data_unplug19 = [[ejes_unplug19[1][i]] for i  in range(len(ejes_unplug19[1]))]
    data_unplug20 = [[ejes_unplug20[1][i]] for i  in range(len(ejes_unplug20[1]))]
    data_plug19 = [[ejes_plug19[1][i]] for i  in range(len(ejes_plug19[1]))]
    data_plug20 = [[ejes_plug20[1][i]] for i  in range(len(ejes_plug20[1]))]

    fig, ax = plt.subplots(1, 1)
    ax.axis('tight')
    ax.axis('off')
    ax.table(cellText=data_unplug19,rowLabels=ejes_unplug19[0],colLabels=["Salidas 19"],loc="center",cellLoc='center')
    fig.tight_layout()
    plt.show()
    
    fig, ax = plt.subplots(1, 1)
    ax.axis('tight')
    ax.axis('off')
    ax.table(cellText=data_unplug20,rowLabels=ejes_unplug20[0],colLabels=["Salidas 20"],loc="center",cellLoc='center')	
    fig.tight_layout()
    plt.show()
    
    fig, ax = plt.subplots(1, 1)
    ax.axis('tight')
    ax.axis('off')
    ax.table(cellText=data_plug19,rowLabels=ejes_plug19[0],colLabels=["Llegadas 19"],loc="center",cellLoc='center')
    fig.tight_layout()
    plt.show()
    
    fig, ax = plt.subplots(1, 1)
    ax.axis('tight')
    ax.axis('off')
    ax.table(cellText=data_plug20,rowLabels=ejes_plug20[0],colLabels=["Llegadas 20"],loc="center",cellLoc='center')	
    fig.tight_layout()
    plt.show()

#----------------------------------------------------------------------------------------------------------

def proceso(rdd19,rdd20,lrdd19,lrdd20, months, archivo_salida):
    estudio_edad(rdd19, rdd20, archivo_salida)
    estudio_usuario_unico(lrdd19, lrdd20, months, archivo_salida)
    estudio_station(rdd19,rdd20, archivo_salida)

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

if __name__ =="__main__":
	if len(sys.argv) <= 1:
		years=[2019,2020] 
	else:
		years=list(map(int, sys.argv[1][1:-1].split(",")))
	if len(sys.argv) <= 2:
		months=[5,6,7,8,9,10] #Los meses de los que hacemos el estudio
		
	else:
		months=list(map(int, sys.argv[2][1:-1].split(",")))

	main(sc, years,months)
from elasticsearch import Elasticsearch
import requests
import time

API_TOKEN = '9e97d8df88698f41ba2bb5e86efc2e4f'

list_film = [
    "Casablanca",
    "Le Parrain",
    "Autant en emporte le vent",
    "Lawrence d'Arabie",
    "Le Magicien d'Oz",
    "Le Lauréat",
    "Sur les quais",
    "La Liste de Schindler",
    "Chantons sous la pluie",
    "La vie est belle",
    "Boulevard du crépuscule",
    "Le Pont de la rivière Kwaï",
    "Certains l'aiment chaud",
    "Star Wars, épisode IV : Un nouvel espoir",
    "Ève",
    "L'Odyssée de l'African Queen ",	
    "Psychose",
    "Le Mécano de la « General »",
    "Chinatown ",
    "Vol au-dessus d'un nid de coucou",
    "Les Raisins de la colère",
    "2001, l'Odyssée de l'espace",
    "Le Faucon maltais",
    "Raging Bull",
    "E.T., l'extra-terrestre",
    "Docteur Folamour",
    "Bonnie et Clyde",
    "Apocalypse Now",
    "Monsieur Smith au Sénat",
    "Le Trésor de la Sierra Madre",
    "Annie Hall",
    "Le Parrain 2",
    "Le train sifflera trois fois",
    "Du silence et des ombres",
    "New York-Miami",
    "Macadam Cowboy",
    "Les Plus Belles Années de notre vie",
    "Assurance sur la mort",
    "Le Docteur Jivago",
    "La Mort aux trousses",
    "West Side Story",
    "Fenêtre sur cour",
    "King Kong",
    "Naissance d'une nation",
    "Un tramway nommé Désir",
    "Orange mécanique",
    "Taxi Driver",
    "Les Dents de la mer",
    "Blanche-Neige et les Sept Nains",
    "Intolerance",
    "Butch Cassidy et le Kid",
    "Le Seigneur des anneaux : La Communauté de l'anneau",
    "Indiscrétions",
    "Tant qu'il y aura des hommes",
    "Amadeus",
    "À l'Ouest, rien de nouveau",
    "La Mélodie du bonheur",
    "MASH",
    "Le Troisième Homme",
    "Fantasia",
    "La Fureur de vivre",
    "Nashville",
    "Les Aventuriers de l'arche perdue",
    "Sueurs froides",
    "Les Voyages de Sullivan",
    "Tootsie",
    "La Chevauchée fantastique",
    "Cabaret",
    "Rencontres du troisième type"
]

es = Elasticsearch("https://es03:9200", ca_certs="http_ca.crt", basic_auth=("elastic", "Kkv8Pxw7wFYgtkt7fx02"))


def getResponseByFilmPage(text,page) :
    print("text == ", text, " page == ",page )
    response = requests.get('https://api.themoviedb.org/3/search/movie?api_key=' + API_TOKEN + '&language=fr&query=' + text + '&page=' + str(page))
    return response.json()

def getResponseByFilm(text) :
    response = requests.get('https://api.themoviedb.org/3/search/movie?api_key=' + API_TOKEN + '&language=fr&query=' + text )
    return response.json()




def ingestionElastic():
    for filmvalue in list_film:
        result_request =getResponseByFilm(filmvalue)
        nombre_total_page=result_request["total_pages"]
        for page in range(nombre_total_page) :
            if(page>0):
                result_request=getResponseByFilmPage(filmvalue,page)
            results = result_request["results"]
            for index,film in enumerate(results) :
                document = {
                    "original_language": film["original_language"],
                    "original_title": film["original_title"],
                    "overview": film["overview"],
                    "popularity": film["popularity"],
                    "poster_path": film["poster_path"],
                    "release_date": film["release_date"],
                    "title": film["title"],
                    "vote_average": film["vote_average"],
                    "vote_count": film["vote_count"]
                }
                print("document == ", document)
                print("release_date == '",document["release_date"],"'",type(document["release_date"]))
                if document["release_date"] !='':
                    es.index(index="film",id= int(((index+1)*(page+2))) ,document=document)
                    time.sleep(100)








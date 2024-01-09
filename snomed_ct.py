import requests
import sys
import json

baseUrl = 'https://browser.ihtsdotools.org/snowstorm/snomed-ct'
edition = 'MAIN'
version = '2019-07-31'

top_conceptId = ["64572001", "49755003", "91723000"]

done_son = set()


def getTaxonomy(id):
    #https://browser.ihtsdotools.org/snowstorm/snomed-ct/browser/MAIN/concepts/49049000
    relationship_target = {}

    if id not in done_son:
        url = baseUrl + '/browser/' + edition + '/concepts/' + id
        #print ("getDisorderTaxonomy", url)
        response = requests.get(url, headers={"user-agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36"})
        data = json.loads(response.text)
        relationships = []
        if 'relationships' in data:
            relationships = data['relationships']
        class_axioms = []
        if len(data['classAxioms']) > 0 and "relationships" in data['classAxioms'][0]: 
            class_axioms = data['classAxioms'][0]['relationships']

        done = set()

        for r in relationships + class_axioms:
            if r["active"] == True:
                fsn = ""
                pt = ""
                conceptId = ""
                target_fsn = ""
                target_pt = ""
                if "type" in r and r["type"]["fsn"]["term"] == "Is a (attribute)" and "target" in r and (r["target"]["fsn"]["term"].endswith("(disorder)") or r["target"]["fsn"]["term"].endswith("(morphologic abnormality)") or r["target"]["fsn"]["term"].endswith("(body structure)")):
                    fsn = r["type"]["fsn"]["term"]
                    pt = r["type"]["pt"]["term"]

                    conceptId = r["target"]["conceptId"]
                    
                    target_fsn = r["target"]["fsn"]["term"]
                    
                    target_pt = r["target"]["pt"]["term"]
                
                    if fsn != "" and pt != "" and conceptId != "" and target_fsn != "" and target_pt != "":
                        relationship_target[(id,conceptId,fsn)] = {"fsn":fsn, "pt":pt, "conceptId":conceptId, "target_fsn":target_fsn, "target_pt":target_pt}

                        if conceptId != "" and conceptId != top_conceptId:
                            
                            if (id, conceptId) not in done:
                                print ("getTaxonomy", id, conceptId)
                                parent = getTaxonomy(conceptId)
                                done.add((id, conceptId))
                                for p in parent:
                                    relationship_target[p] = parent[p]

        done_son.add(id)
    return relationship_target



def getOnotologyById(id):
    #https://browser.ihtsdotools.org/snowstorm/snomed-ct/browser/MAIN/concepts/254837009
    url = baseUrl + '/browser/' + edition + '/concepts/' + id
    #print (url)
    response = requests.get(url, headers={"user-agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36"})
    data = json.loads(response.text)
    print (json.dumps(data))
    if 'relationships' in data:
        relationships = data['relationships']
    class_axioms = []
    if len(data['classAxioms']) > 0 and "relationships" in data['classAxioms'][0]: 
        class_axioms = data['classAxioms'][0]['relationships']


    relationship_target = {}

    for r in relationships + class_axioms:
        if r["active"] == True:
            fsn = ""
            pt = ""
            conceptId = ""
            target_fsn = ""
            target_pt = ""
            if "type" in r:
                if "fsn" in r["type"]:
                    fsn = r["type"]["fsn"]["term"]
                if "pt" in r["type"]:
                    pt = r["type"]["pt"]["term"]

            if "target" in r:
                if "conceptId" in r["target"]:
                    conceptId = r["target"]["conceptId"]
                if "fsn" in r["target"]:
                    target_fsn = r["target"]["fsn"]["term"]
                if "pt" in r["target"]:
                    target_pt = r["target"]["pt"]["term"]
            
            if fsn != "" and pt != "" and conceptId != "" and target_fsn != "" and target_pt != "":
                relationship_target[(id,conceptId,fsn)] = {"fsn":fsn, "pt":pt, "conceptId":conceptId, "target_fsn":target_fsn, "target_pt":target_pt}

                if conceptId != "" and conceptId not in top_conceptId and (r["type"]["fsn"]["term"] == "Is a (attribute)" or r["type"]["fsn"]["term"] == "Finding site (attribute)") and (target_fsn.endswith("(disorder)") or target_fsn.endswith("(morphologic abnormality)") or target_fsn.endswith("(body structure)")):
                    
                    #print (conceptId)
                    parent = getTaxonomy(conceptId)

                    for p in parent:
                        relationship_target[p] = parent[p]

    return relationship_target



#Prints number of concepts with descriptions containing the search term
def getConceptIdByTerm(searchTerm):
    url = baseUrl + '/browser/' + edition + '/descriptions?term=' + searchTerm + '&conceptActive=true&lang=english&skipTo=0&returnLimit=3'
    #print (url)
    response = requests.get(url, headers={"user-agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36"})
    #print (response.text)
    data = json.loads(response.text)

    items = data['items']
    
    for i in items:
        term = i['term']

        if term.lower() == searchTerm.lower():
            #print (i)
            if "concept" in i and "id" in i["concept"]:
                return i["concept"]["id"]
    return None



if __name__ == "__main__":
    getOnotologyById("11381005")
    #id = getConceptIdByTerm('Breast Cancer')
    #print (id)
    #id = getConceptIdByTerm(sys.argv[1])
    #id = getConceptIdByTerm('Parkinson Disease')
    #print (getOnotologyById(id))
    #getDescriptionsByStringFromProcedure('heart', 'procedure')

    #print (getDisorderTaxonomy("254837009"))
    #print (getOnotologyById("254837009"))
    #print (getOnotologyById("24072005"))
    #print (getOnotologyById("37796009"))
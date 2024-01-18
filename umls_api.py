import requests
import os
from threading import Thread
import queue
umls_token = os.getenv('UMLS')

#https://uts-ws.nlm.nih.gov/rest/content/current/source/MSH/D014839/relations?apiKey=

#https://uts-ws.nlm.nih.gov/rest/content/current/source/HPO/HP:0002013/relations?includeRelationLabels=CHD&apiKey=

def get_parent_HPO(hpo_id: str, umls_token: str):
    url = f"https://uts-ws.nlm.nih.gov/rest/content/current/source/HPO/{hpo_id}/relations?includeRelationLabels=CHD&includeAdditionalRelationLabels=isa&apiKey={umls_token}"
    response = requests.get(url).json()

    results = []
    
    if "result" not in response:
        return results

    else:
        for r in response["result"]:
            if "relatedId" in r and "relatedIdName" in r:
                parent_hpo = r["relatedId"].split("/")[-1]
                parent_name = r["relatedIdName"]
                if parent_hpo.startswith("HP:"):
                    results.append((parent_hpo, parent_name))
        
        return results

def recursive_get_parent_HPO(hpo_id: str, umls_token: str):

    in_queue = queue.Queue()
    out_queue = queue.Queue()

    threads = 1

    def work():
        while True:
            son_hpo_id = in_queue.get()
            print ("son_hpo_id", son_hpo_id, get_parent_HPO(son_hpo_id, umls_token))

            for entity in get_parent_HPO(son_hpo_id, umls_token):
                print (entity)
                out_queue.put((son_hpo_id, entity[0], entity[1]))
                in_queue.put(entity[0])

       
            in_queue.task_done()
    
    for i in range(threads):
        
        t = Thread(target=work)
        t.daemon = True
        t.start()

    
    in_queue.put(hpo_id)
        
    
    in_queue.join()   
 
    result = []
    
    while not out_queue.empty():
        
        result.append( out_queue.get())

    #print result
    return result


    # return history

def get_may_be_treated_by(msh_id: str, umls_token: str):
    url = f"https://uts-ws.nlm.nih.gov/rest/content/current/source/MSH/{msh_id}/relations?includeAdditionalRelationLabels=may_be_treated_by&apiKey={umls_token}"
    response = requests.get(url).json()

    #print (response)
    may_be_treated_by_drugs = []
    
    for r in response["result"]:
        if "relatedId" in r and "relatedIdName" in r:
            id = r["relatedId"].split("/")[-1]
            name = r["relatedIdName"]

            may_be_treated_by_drugs.append((id, name))

    return may_be_treated_by_drugs

if __name__ == "__main__":
    #print(get_parent_HPO("HP:0000001", umls_token))
    print(recursive_get_parent_HPO("HP:0011458", umls_token))
    #print(get_may_be_treated_by("D014839", umls_token))
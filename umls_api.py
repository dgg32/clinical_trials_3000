import requests
import os
from threading import Thread
import queue
umls_token = os.getenv('UMLS')

#https://uts-ws.nlm.nih.gov/rest/content/current/source/MSH/D014839/relations?apiKey=

#https://uts-ws.nlm.nih.gov/rest/content/current/source/HPO/HP:0002013/relations?includeRelationLabels=CHD&apiKey=


def get_all_items(source_name: str, id: str, extra_parameter: str, umls_token: str):
    url = f"https://uts-ws.nlm.nih.gov/rest/content/current/source/{source_name}/{id}/relations?{extra_parameter}&apiKey={umls_token}"

    response = requests.get(url).json()

    page_count = int(response["pageCount"])

    #print ("page_count", page_count)
    
    results = get_item(response)

    for i in range(2, page_count+1):
        new_url = url + f"&pageNumber={i}"

        new_response = requests.get(new_url).json()

        #print ("new_url", new_url)
        #print ("new_response", new_response)

        results += get_item(new_response)
    
    return results



def get_item(response: str):

    result = []
    
    if "result" not in response:
        return result

    else:
        for r in response["result"]:
            if r["classType"] == "AtomClusterRelation" and "relatedId" in r and "relatedIdName" in r:
                item_id = r["relatedId"].split("/")[-1]
                item_name = r["relatedIdName"]


                result.append((item_id, item_name))

        
        return result

def recursive_get_parent_HPO(hpo_id: str, umls_token: str):

    in_queue = queue.Queue()
    out_queue = queue.Queue()

    threads = 1

    def work():
        while True:
            son_hpo_id = in_queue.get()
            #print ("son_hpo_id", son_hpo_id, get_all_items("HPO", son_hpo_id, "includeRelationLabels=CHD&includeAdditionalRelationLabels=isa", umls_token))

            for entity in get_all_items("HPO", son_hpo_id, "includeRelationLabels=CHD&includeAdditionalRelationLabels=isa", umls_token):
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




if __name__ == "__main__":
    #print(get_parent_HPO("HP:0000001", umls_token))
    #print(recursive_get_parent_HPO("HP:0011458", umls_token))

#### get may_be_prevented_by drugs of a disease
    result = get_all_items("MSH", "D014839", "includeAdditionalRelationLabels=may_be_prevented_by", umls_token)

    assert len(result) == 40
    print (result)
    
#### recursive get all parent HPO
    print ("\n\n")
    print(recursive_get_parent_HPO("HP:0011458", umls_token))
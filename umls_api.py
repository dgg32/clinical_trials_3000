import requests
import os

umls_token = os.getenv('UMLS')

#https://uts-ws.nlm.nih.gov/rest/content/current/source/MSH/D014839/relations?apiKey=

#https://uts-ws.nlm.nih.gov/rest/content/current/source/HPO/HP:0002013/relations?includeRelationLabels=CHD&apiKey=

def get_parent_HPO(hpo_id: str, umls_token: str):
    url = f"https://uts-ws.nlm.nih.gov/rest/content/current/source/HPO/{hpo_id}/relations?includeRelationLabels=CHD&includeAdditionalRelationLabels=isa&apiKey={umls_token}"
    response = requests.get(url).json()

    results = []
    
    for r in response["result"]:
        if "relatedId" in r and "relatedIdName" in r:
            parent_hpo = r["relatedId"].split("/")[-1]
            parent_name = r["relatedIdName"]
            if parent_hpo.startswith("HP:"):
                results.append((parent_hpo, parent_name))
    
    return results

def recursive_get_parent_HPO(hpo_id: str, umls_token: str):
    # history = []

    # results = get_parent_HPO(hpo_id, umls_token)

    # for r in results:
    #     parent_hpo = r[0]
    #     parent_name = r[1]
    #     history.append((parent_hpo, parent_name))

            
    #     if parent_hpo != "HP:0000001":
    #         sub_results = recursive_get_parent_HPO(parent_hpo, umls_token)
    #         break


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
    print(get_parent_HPO("HP:0011458", umls_token))
    #print(recursive_get_parent_HPO("HP:0011458", umls_token))
    #print(get_may_be_treated_by("D014839", umls_token))
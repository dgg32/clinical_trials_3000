import pycurl
import json
from subprocess import check_output
from io import BytesIO

gcp_token = check_output(["gcloud", "auth", "print-access-token"], encoding='UTF-8').strip()

def get_healthcare_json(text: str, gcp_token: str) -> str:
    data = {'documentContent': f'{text}'}
    post_data = json.dumps(data)
    headers = ['Content-Type: application/json', f'Authorization: Bearer {gcp_token}']
    buffer = BytesIO()

    c = pycurl.Curl()
    c.setopt(c.URL, 'https://healthcare.googleapis.com/v1/projects/neo4j-dashboard/locations/us-central1/services/nlp:analyzeEntities')
    c.setopt(c.POSTFIELDS, post_data)
    c.setopt(c.HTTPHEADER, headers)
    c.setopt(c.WRITEDATA, buffer)
    c.perform()
    c.close()

    response = buffer.getvalue().decode('utf-8')

    return response


def get_json(json_form: str, category: str, corrections: dict, offset_text) -> str:
    

    linkedEntities = {entity["entityId"]: {"preferredTerm": entity["preferredTerm"], "vocabularyCodes": entity["vocabularyCodes"]} 
                  for entity in json_form["entities"]}
    
    print (linkedEntities)
    temp_raw_ner = {}
    for entity in json_form["entityMentions"]:
        
        if entity["type"] == category and entity["confidence"] > 0.4 and "linkedEntities" in entity:
            print (entity["text"], entity["linkedEntities"], entity["confidence"])
            begin_offset = entity["text"]["beginOffset"]
            stop = begin_offset + len(entity["text"]["content"])


            for i, iterator in enumerate(offset_text):
                if iterator[0] <= begin_offset and begin_offset < iterator[1]:
                    if i not in temp_raw_ner:
                        temp_raw_ner[i] = []
                        temp_raw_ner[i].append((entity["text"]["content"], entity["linkedEntities"][0]["entityId"], entity["confidence"], begin_offset, stop))

                    else:
                        found = False
                        for existing_entity in temp_raw_ner[i]:
                            if existing_entity[3] <=  stop and begin_offset <= existing_entity[4]:
                                if entity["confidence"] > existing_entity[2]:
                                    found = True
                                    temp_raw_ner[i].remove(existing_entity)
                                    temp_raw_ner[i].append((entity["text"]["content"], entity["linkedEntities"][0]["entityId"], entity["confidence"], begin_offset, stop))
                                    break

                        if found == False:
                            temp_raw_ner[i].append((entity["text"]["content"], entity["linkedEntities"][0]["entityId"], entity["confidence"], begin_offset, stop))
                    break

    #print ("temp_raw_ner", temp_raw_ner)
    content = []
    for i, entity in enumerate(offset_text):
        if entity[2] in corrections:
            content.append({entity[2]: corrections[entity[2]]})

        else:
            resolved_entities = []

            done = set()
            if i in temp_raw_ner:
                
                for j in temp_raw_ner[i]:

                    entityids = j[1]

                    if entityids in done:
                        continue
                    else:
                        done.add(entityids)

                        preferredTerms = linkedEntities[entityids]["preferredTerm"]

                        HPO = []
                        MSH = []


                        for id in linkedEntities[entityids]["vocabularyCodes"]:
                            if id.startswith("HPO"):
                                HPO.append(id)
                            elif id.startswith("MSH"):
                                MSH.append(id)

                        resolved_entities.append({"preferredTerms": preferredTerms, "entityids": entityids, "HPO": HPO, "MSH": MSH})
            else:
                resolved_entities.append({})
        
            content.append({entity[2]: resolved_entities})
    return content

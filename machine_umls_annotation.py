import umls_api
import pandas as pd
import yaml
#import healthcare_nlp
import json
from tqdm import tqdm
import re
import sys
import os

def machine_assign_umls(filename, query_column="Medicine or Vaccine (generic name)"):
    with open("config.yaml", "r") as stream:
        try:
            PARAM = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)

    umls_token = PARAM["umls_token"]
    sheet_id = PARAM["google_sheet_id"]
    sheet_name = PARAM["google_sheet_node_name"]
    manual_curation_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"

    manual_curation_df = pd.read_csv(manual_curation_url)

    manual_curation = {}

    for index, row in manual_curation_df.iterrows():
        if pd.notna(row["correct_cui"]):
            manual_curation[row["substance"].lower()] = {"correct_cui": [x.strip() for x in row["correct_cui"].split(";")], "preferred_umls_name": [x.strip() for x in row["preferred_umls_name"].split(";")]}
        else:
            manual_curation[row["substance"].lower()] = {"correct_cui": [], "preferred_umls_name": []}
    
    df = pd.read_csv(filename, sep='\t', lineterminator='\n')
    df.columns = [x.strip() for x in df.columns]
    df = df.replace({r'(.*)\r': r'\1'}, regex=True)


    substance_umls_hit = {}
    substance_umls_partial_hit = {}

    if "substance_umls_hit.json" in os.listdir("temp"):
        with open("temp/substance_umls_hit.json", "r") as f:
            substance_umls_hit = json.load(f)

    if "substance_umls_partial_hit.json" in os.listdir("temp"):
        with open("temp/substance_umls_partial_hit.json", "r") as f:
            substance_umls_partial_hit = json.load(f)

    print ("substance_umls_hit", len(substance_umls_hit))
    unique_subtance = set()


    for x in pd.unique(df[query_column]):
        if isinstance(x, str):
            if x.lower() in manual_curation:
                pass
            else:
                if "," in x or "/" in x or ";" in x or "+" in x:
                    sep = ","
                    if "," in x:
                        sep = ","
                    elif ";" in x:
                        sep = ";"
                    elif "/" in x:
                        sep = "/"
                    elif "+" in x:
                        sep = "+"
                    for y in x.split(sep):
                        if y.strip() != "":
                            if y.strip().lower() not in manual_curation and y.strip().lower() not in substance_umls_hit:
                                unique_subtance.add(y.strip().lower())
                            else:
                                if y.strip().lower() in manual_curation:
                                    if len(manual_curation[y.strip().lower()]["correct_cui"]) > 0:
                                        substance_umls_hit[y.strip().lower()] = {"cui": manual_curation[y.strip().lower()]["correct_cui"][0], "name": manual_curation[y.strip().lower()]["preferred_umls_name"][0], "semantic_type": ["manual_curation"]}
                                    else:
                                        substance_umls_hit[y.strip().lower()] = {"cui": "", "name": "", "semantic_type": ["manual_curation"]}
                            

                else:
                    unique_subtance.add(x.lower().strip())
    


    allowed_semantic_type = {"Organic Chemical", "Pharmacologic Substance", "Amino Acid, Peptide, or Protein", "Inorganic Chemical", "Indicator, Reagent, or Diagnostic Aid"}



    for row in tqdm(unique_subtance):
        #print ("row", row)
        try:


            #print ("submit_row", submit_row)
            if row == "":
                substance_umls_hit[row] = {"cui": "", "name": "", "semantic_type": ["manual_curation"]}

            else:
                if row in substance_umls_hit:
                    pass
                else:
                    results = umls_api.search(row, umls_token)
                    #print ("submit_row", submit_row, "results", results)

                    first_hit = ""
                    first_semantic_type = []
                    got_hit = False
                    for index, result in enumerate(results):
                        
                        semantic_type = umls_api.get_semantic_type(result["cui"], umls_token)
                        #print (result, semantic_type)
                        if index == 0:
                            first_hit = result
                            first_semantic_type = semantic_type

                    
                        if len(set(semantic_type).intersection(allowed_semantic_type)) > 0:
                            substance_umls_hit[row] = result
                            substance_umls_hit[row]["semantic_type"] = semantic_type
                            got_hit = True
                            break


                    if got_hit == False and first_hit != "":
                        substance_umls_partial_hit[row] = first_hit
                        substance_umls_partial_hit[row]["semantic_type"] = first_semantic_type
        except Exception as e:
            print ("error: ", e, row)


    with open("temp/substance_umls_hit.json", "w") as f:
        json.dump(substance_umls_hit, f)

    for_partial_search = unique_subtance.difference(set(substance_umls_hit.keys())).difference(set(substance_umls_partial_hit.keys()))

    for row in tqdm(for_partial_search):
        try:
            if row in substance_umls_partial_hit:
                pass
            
            else:
                results = umls_api.search(row, umls_token, True)
                first_hit = ""
                first_semantic_type = ""
                got_hit = False
                
                for index, result in enumerate(results):
                    semantic_type = umls_api.get_semantic_type(result["cui"], umls_token)

                    if index == 0:
                        first_hit = result
                        first_semantic_type = semantic_type

                    if len(set(semantic_type).intersection(allowed_semantic_type)) > 1:
                        substance_umls_partial_hit[row] = result
                        substance_umls_partial_hit[row]["semantic_type"] = semantic_type
                        got_hit = True
                        break

                if got_hit == False and first_hit != "" and first_semantic_type != "":
                    substance_umls_partial_hit[row] = first_hit
                    substance_umls_partial_hit[row]["semantic_type"] = first_semantic_type
        except Exception as e:
            print ("error: ", e, row)
    

    with open("temp/substance_umls_partial_hit.json", "w") as f:
        json.dump(substance_umls_partial_hit, f)
    
    #print ("substance_umls_hit", substance_umls_hit)
    #print ("substance_umls_partial_hit", substance_umls_partial_hit)

    for index, row in df.iterrows():
        if isinstance(row[query_column], str):
            annotated_cui = []
            annotated_names = []
            annotated_semantic_type = []
            annotated_high_confidence = []
            
            if row[query_column].lower() in manual_curation:
                annotated_cui = manual_curation[row[query_column].lower()]["correct_cui"]
                annotated_names = manual_curation[row[query_column].lower()]["preferred_umls_name"]
                annotated_semantic_type = ["manual_curation"]
                annotated_high_confidence = [True]

            else:
                
                if "," in row[query_column] or "/" in row[query_column] or ";" in row[query_column] or "+" in row[query_column]:
                    sep = ","
                    if "," in row[query_column]:
                        sep = ","
                    elif ";" in row[query_column]:
                        sep = ";"
                    elif "/" in row[query_column]:
                        sep = "/"
                    elif "+" in row[query_column]:
                        sep = "+"
                    for y in row[query_column].split(sep):
                        substance = y.strip().lower()
                        if substance in substance_umls_hit:
                            cui = substance_umls_hit[substance]["cui"]
                            if cui != "" and cui not in annotated_cui:
                                annotated_cui.append(substance_umls_hit[substance]["cui"])
                                annotated_names.append(substance_umls_hit[substance]["name"])
                                annotated_semantic_type.append("|".join(substance_umls_hit[substance]["semantic_type"]))
                                annotated_high_confidence.append(True)
                        elif substance in substance_umls_partial_hit:
                            cui = substance_umls_partial_hit[substance]["cui"]
                            if cui != "" and cui not in annotated_cui:
                                annotated_cui.append(substance_umls_partial_hit[substance]["cui"])
                                annotated_names.append(substance_umls_partial_hit[substance]["name"])
                                annotated_semantic_type.append("|".join(substance_umls_partial_hit[substance]["semantic_type"]))
                                annotated_high_confidence.append(False)
                else:
                    substance = row[query_column].lower()
                    if substance in substance_umls_hit:
                        cui = substance_umls_hit[substance]["cui"]
                        if cui != "" and cui not in annotated_cui:
                            annotated_cui.append(substance_umls_hit[substance]["cui"])
                            annotated_names.append(substance_umls_hit[substance]["name"])
                            annotated_semantic_type.append("|".join(substance_umls_hit[substance]["semantic_type"]))
                            annotated_high_confidence.append(True)
                    elif substance in substance_umls_partial_hit:
                        cui = substance_umls_partial_hit[substance]["cui"]
                        if cui != "" and cui not in annotated_cui:
                            annotated_cui.append(substance_umls_partial_hit[substance]["cui"])
                            annotated_names.append(substance_umls_partial_hit[substance]["name"])
                            annotated_semantic_type.append("|".join(substance_umls_partial_hit[substance]["semantic_type"]))
                            annotated_high_confidence.append(False)
            
            #print (annotated_cui, annotated_names, annotated_semantic_type, annotated_high_confidence)
            
            df.at[index, "CUI_umls"] = ";".join(annotated_cui)
            df.at[index, "preferred_umls_name"] = ";".join(annotated_names)
            df.at[index, "semantic_type"] = ";".join(annotated_semantic_type)
            df.at[index, "high_confidence"] = ";".join([str(x) for x in annotated_high_confidence])

    return df

if __name__ == "__main__":

    query_column = "Medicine or Vaccine (generic name)"
    df = machine_assign_umls("neo4j/trials.tsv", query_column)

    df[df['CUI_umls'] != ""].to_csv("temp/trials_umls.tsv", sep='\t', index=False)

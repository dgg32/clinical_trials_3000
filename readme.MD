

# Introduction
  

This repository contains code and data for my article "[Getting Insights from 3,000+ Clinical Trials in a Knowledge Graph](https://medium.com/@dgg32/getting-insights-from-3-000-clinical-trials-in-a-knowledge-graph-df17e55fa860)". 

1. The Python scripts are for the chatbot.

  

# Prerequisite

Neo4j Desktop or AuraDB

GCP account

UMLS free tier


Enbale Vertex AI API in the GCP project, increase its quota (default 5)

501 VECTOR INDEX is not available in granular instances in preview 12: VECTOR INDEX is not available in granular instances in preview

# Run
1. Set up the databases as described in the article. 
  
2. Install the necessary Python libraries (requirements.txt) and gcloud CLI

3. Fill the necessary API key information in config.yaml

4. Run 
```
clinical_trials_to_kg.ipynb
```

and 

```
get_meta_data_experiment.ipynb
```

to convert the clinical data to TSV

5. Import the data into Neo4j

6. Run 
```
to_bigquery.ipynb
```
to export data to GCP and to create dashboard

## Authors

  

*  **Sixing Huang** - *Concept and Coding*

  

## License

  

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details

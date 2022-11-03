# Réponses

## Data pipeline

La pipeline se compose de trois étapes:
    - load_and_norm_X : dans cette fonction on peut définir les régles de nettoyage et validation des inputs (Great Expectation, TDDA)
    - search_for_drugs_and_save: cette étape et encapsule l'algorithme d'identifications des drugs dans une source. pour la simplicité j'ai utilisé une regex, à revoir si la list des drugs est plus longue. cette étape écrit les données sur disque afin de creer un checkpoint. Ceci est pratique pour une reprise sur erreur.
    - generate_dag_and_save: cette étape construit le dag à partir des résultats de l'étape préceédente. j'ai choici de modéliser le DAG dans une table où chaque ligne est une arrete entre un noeud de drug et un noeud de pubmed/clinic_trials. cette modelisation est requetable en sql et on peut facilement la transformer en deux tables de Edges et Vertexes, si on souhaite utiliser le framework GraphX de Spark pour faire des operations sur les graphs.

### Setup

```bash
pip install -f requirements.txt
```

### Exécution

```bash
# Run pipeline
python src/main.py pipeline

# Run find publication with most drug mentions
python src/main.py topdrugs
```

### Pour aller plus loin

**Avant de traiter la question de la volumétrie il est essentiel de mettre en place un pipeline de Data Quality. 
J'ai écrit un article sur ce sujet visible [ici](https://www.quantmetry.com/blog/introduire-des-tests-a-un-pipeline-de-donnees/)**

Les améliorations dependent de l'infra en place, pour traiter des grandes quantités de données.
Si on est sur une architecture serverless, par exemple sur AWS, une stack de S3 -> SQS -> Lambda est adaptée pour ingérer un grand nombre de fichiers. par contre si les fichiers sont volumineux, on peut partitioner les fichiers (par date, par id ...) pour obtenir des fichiers de la meme taille, mais gérable dans une mémoire de lambda. Dans les deux cas il est possible d'utliser le code "sans modification"
Dans une infra classique (un cluster de calcul), ce code basé sur le framework Pandas n'est pas adapté pour utiliser le le potentiel du cluster à 100%. Il est judicieux de passer sur framework plus adapté comme Spark.

## SQL

### 1ere partie

```SQL
SELECT date, SUM(prod_price * prod_qty) AS ventes
FROM TRANSACTION 
WHERE date BETWEEN '01/01/2019' AND '31/12/2019'
GROUP BY date;
```

### 2eme partie

```SQL
SELECT client_id, 
    SUM(prod_price * prod_qty) FILTER (WHERE product_type = 'MEUBLE' ) AS ventes_meuble,
    SUM(prod_price * prod_qty) FILTER (WHERE product_type = 'DECO' ) AS ventes_deco
FROM 
    TRANSACTION LEFT JOIN PRODUCT_NOMENCLATURE ON TRANSACTION.product_id = PRODUCT_NOMENCLATURE.prod_id
WHERE date BETWEEN '01/01/2020' AND '31/12/2020'
GROUP BY client_id, product_type;
```

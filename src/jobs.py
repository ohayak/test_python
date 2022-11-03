import re
import pandas as pd

def load_and_norm_trials(fm):
    """
    Cleaning and normalization rules for clinical trials data
    """
    df = fm.load('clinical_trials.csv', parse_dates=['date'], infer_datetime_format=True, dayfirst=True) \
        .rename(columns={'scientific_title': 'title'}) \
        .dropna(subset=['id', 'title'])
    return df


def load_and_norm_pubmeds(fm):
    """
    Cleaning and normalization rules for PubMed data
    """
    df = fm.load('pubmed.csv', parse_dates=['date'], infer_datetime_format=True, dayfirst=True) \
        .dropna(subset=['id', 'title'])
    return df

def search_for_drugs_and_save(fm, drugs_df, df, source_name):
    """
    Drugs searching function
    """
    all_drugs_regex = "|".join(drugs_df['drug'])
    found_drugs_ss = df['title'].str \
        .findall(all_drugs_regex, re.IGNORECASE) \
        .explode() \
        .str.upper() \
        .dropna()
    found_drugs_ss.name = 'drug'
    result = pd.merge(df, found_drugs_ss, left_index=True, right_index=True) \
        .merge(drugs_df, on='drug')
    fm.save(result[['id', 'date', 'drug', 'atccode']], f'{source_name}.csv', index=False)

def generate_dag_and_save(source_fm, dest_fm, sources):
    """
    Dag generation
    """
    source_dfs = []
    for source in sources:
        source_df = source_fm.load(f'{source}.csv')
        source_df['source'] = source
        source_dfs.append(source_df)

    result = pd.concat(source_dfs)
    dest_fm.save(result, f'dag.json', orient="records", indent=2)
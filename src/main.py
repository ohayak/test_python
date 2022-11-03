from jobs import generate_dag_and_save, load_and_norm_pubmeds, load_and_norm_trials, search_for_drugs_and_save
from utils.files import FileManager
from argparse import ArgumentParser






if __name__ == "__main__" :
    parser = ArgumentParser()
    subparsers = parser.add_subparsers(dest="action")
    subparsers.add_parser("pipeline")
    subparsers.add_parser("topdrugs")
   
    args = parser.parse_args()

    inputs_fm = FileManager('inputs/')
    outputs_fm = FileManager('outputs/')
    tmp_fm = FileManager('outputs/tmp/')

    if args.action == 'pipeline':
        trials_df = load_and_norm_trials(inputs_fm)
        pubmed_df = load_and_norm_pubmeds(inputs_fm)
        drugs_df = inputs_fm.load('drugs.csv')

        search_for_drugs_and_save(tmp_fm, drugs_df, trials_df, 'clinical_trials')
        search_for_drugs_and_save(tmp_fm, drugs_df, pubmed_df, 'pubmed')

        generate_dag_and_save(tmp_fm, outputs_fm, ['clinical_trials', 'pubmed'])
        print("Run pipeline done")

    if args.action == 'topdrugs':
        dag_df = outputs_fm.load('dag.json', orient="records")
        count_df = dag_df.groupby('id')['drug'] \
            .agg('count') \
            .reset_index(name='drugs') \
            .sort_values(by='drugs', ascending=False)
        print(count_df.iloc[:1])
        print("Run topdrus done")



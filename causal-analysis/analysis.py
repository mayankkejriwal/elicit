import numpy as np
import pandas as pd
from pandas import DataFrame
import json

elicit_path='/Users/mayankkejriwal/datasets/elicit/causal-analysis/'

def analysis_on_api_series(in_file=elicit_path+'api_datasets_series.json', condensed_output=elicit_path+'api_condensed_output.json',
                           filtered_condensed_output=elicit_path + 'api_filtered_condensed_output.json'):
    series = json.load(open(in_file, 'r'))
    condensed_dict = _condense_api_dataset_series(series)
    print len(condensed_dict.keys())
    json.dump(condensed_dict, open(condensed_output, 'w'))
    filter_condensed_dict(condensed_dict)
    json.dump(condensed_dict, open(filtered_condensed_output, 'w'))
    print len(condensed_dict.keys())
    correlation_analysis(condensed_dict)


def correlation_analysis(condensed_dict, output_file=elicit_path+'api_correlations.csv'):
    df = pd.DataFrame.from_dict(data=condensed_dict)
    print len(df['/Users/mayankkejriwal/datasets/elicit/causal-analysis/elicit_alignment/m5/datasets/api_ner_ds2_v2/example/API_NER_DS2_en_csv_v2.csv.json\tEP.PMP.SGAS.CD'])
    print len(df.columns)
    # arr = list()
    # keys = sorted(list(condensed_dict.keys()))
    # for k in keys:
    #     arr.append(np.array(condensed_dict[k]))
    #
    # df = pd.DataFrame(data=np.array(arr))
    # print df.describe()
    correlation = df.corr()
    print len(correlation.columns)
    correlation.to_csv(output_file)



def filter_condensed_dict(condensed_dict):
    """
    modifies in place. currently, only filter based on missing values
    :param condensed_dict:
    :return:
    """
    keys_to_remove = set()
    for k, v in condensed_dict.items():
        if len(v)-v.count('') <= 5:
            keys_to_remove.add(k)

    for k in keys_to_remove:
        del condensed_dict[k]

    print 'removed ',str(len(keys_to_remove)),' items from condensed dict...',str(len(condensed_dict.keys())),' items remaining'


def _condense_api_dataset_series(series):
    new_dict = dict()
    year_list = _get_sorted_year_list(series)
    # print year_list
    datasets = sorted(series.keys())
    for d in datasets:
        indicators = sorted(series[d].keys())

        for ind in indicators:
            new_dict[d + '\t' + ind] = list()
            for y in year_list:
                if y not in series[d][ind]:
                    series[d][ind][y] = np.nan # fill in missing years with explicit missing values
                new_dict[d + '\t' + ind].append(series[d][ind][y])


    return new_dict

def _get_sorted_year_list(series):
    big_set = set()
    for k, v in series.items():
        for k1, v1 in v.items():
            big_set = big_set.union(set(v1.keys()))

    print 'num. unique years...',str(len(big_set))
    return sorted(list(big_set))

analysis_on_api_series()
import json


elicit_path='/Users/mayankkejriwal/datasets/elicit/causal-analysis/'
dataset_path = elicit_path+'elicit_alignment/m5/datasets/'


def _construct_api_datasets_paths():
    paths = list()
    paths.append(dataset_path+'api_ben_ds2_v2/example/API_BEN_DS2_en_csv_v2.csv.json')
    paths.append(dataset_path + 'api_cmr_ds2_v2/example/API_CMR_DS2_en_csv_v2.csv.json')
    paths.append(dataset_path + 'api_ner_ds2_v2/example/API_NER_DS2_en_csv_v2.csv.json')
    paths.append(dataset_path + 'api_nga_ds2_v2/example/API_NGA_DS2_en_csv_v2.csv.json')
    paths.append(dataset_path + 'api_tcd_ds2_v2/example/API_TCD_DS2_en_csv_v2.csv.json')
    return paths

api_datasets = _construct_api_datasets_paths()
biomass_dataset = dataset_path+'biomass_production/example/biomass_production_adm_2.xlsx.json'
wfpvam_foodprices_dataset = dataset_path+'wfpvam_foodprices/example/WFPVAM_FoodPrices_05-12-2017.xlsx.json' # needs special processing for correct parsing


def _element_count(time_series=elicit_path+'API_NGA_DS2_en_csv_v2.csv.json'):

    obj = json.load(open(time_series, 'r'))
    print len(obj[0])


def extract_time_series_all_api_files(output_file=elicit_path+'api_datasets_series.json'):
    dataset_id_ts_dict = dict()
    for time_series in api_datasets:
        obj = json.load(open(time_series, 'r'))
        id_ts_dict = dict()
        for t_obj in obj[0]:
            ts = t_obj['ts']
            indicator = t_obj['metadata']['indicator_code']
            if indicator not in id_ts_dict:
                id_ts_dict[indicator] = dict()
            for pair in ts:
                if pair[0] in id_ts_dict[indicator]:
                    raise Exception
                else:
                    id_ts_dict[indicator][pair[0]] = pair[1]
        dataset_id_ts_dict[time_series] = id_ts_dict

    _analyze_id_ts_dict(dataset_id_ts_dict[api_datasets[0]])
    json.dump(dataset_id_ts_dict, open(output_file, 'w'))


def extract_time_series_api_biomass_file(time_series=elicit_path+'API_NGA_DS2_en_csv_v2.csv.json'):
    obj = json.load(open(time_series, 'r'))
    id_ts_dict = dict()
    for t_obj in obj[0]:
        ts = t_obj['ts']
        indicator = t_obj['metadata']['indicator_code']
        if indicator not in id_ts_dict:
            id_ts_dict[indicator] = dict()
        for pair in ts:
            if pair[0] in id_ts_dict[indicator]:
                raise Exception
            else:
                id_ts_dict[indicator][pair[0]] = pair[1]

    print id_ts_dict['SP.POP.4549.FE.5Y']['2012']
    _analyze_id_ts_dict(id_ts_dict)


def _analyze_id_ts_dict(id_ts_dict):
    #first let's see what's the merged keyset of times
    big_keys = set()
    for k, v in id_ts_dict.items():
        big_keys=big_keys.union(set(v.keys()))

    print sorted(list(big_keys))


def extract_time_series_wfpvam_foodprices(time_series=wfpvam_foodprices_dataset):
    obj = json.load(open(time_series, 'r'))
    id_ts_dict = dict()
    for t_obj in obj[0]:
        ts = t_obj['ts']
        indicator = t_obj['metadata']['indicator_code'] # this is a problem, investigate...
        if indicator not in id_ts_dict:
            id_ts_dict[indicator] = dict()
        for pair in ts:
            import re
            pair[0] = re.split(' ',pair[0])[1]
            # print pair[0]
            if pair[0] in id_ts_dict[indicator]:
                raise Exception
            else:
                id_ts_dict[indicator][pair[0]] = pair[1]
    _analyze_id_ts_dict(id_ts_dict)


# extract_time_series_wfpvam_foodprices()
extract_time_series_all_api_files()
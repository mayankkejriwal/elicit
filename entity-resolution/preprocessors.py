import codecs, re, json, gzip
import csv
import glob
from jsonpath_rw import jsonpath, parse

elicit_path='/Users/mayankkejriwal/datasets/elicit/'

#this might not be the right file. let's wait for aditya to crystallize
def extract_nigeria_events_from_gtd(input_file=elicit_path+'gtd_13to16_0617dist-2.csv', output_file=elicit_path+'event-ER/gtd-dist2-nigeria.csv'):
    out = codecs.open(output_file, 'w') # don't use utf-8 for reading or writing, it'll crash
    with codecs.open(input_file, 'rb') as f:
        header = True

        for line in f:
            try:
                if header is True:
                    header = False
                    out.write(line[0:-1]+'\n')
                else:

                        fields = re.split(',',line[0:-1])
                        if fields[8] == 'Nigeria':
                            out.write(line[0:-1]+'\n')
            except Exception as e:
                    print e
                    print line
    out.close()


def acled_gtd_sample_printing(acled=elicit_path+'aditya-3jan-2018/acleddata.jl.gz',gtd=elicit_path+'aditya-3jan-2018/elicit_gtd.jl.gz',
                              output_sample_acled=elicit_path+'aditya-3jan-2018/acleddata-sample.json',
                              output_sample_gtd=elicit_path + 'aditya-3jan-2018/gtd-sample.json'):
    out = codecs.open(output_sample_acled, 'w')
    with gzip.open(acled, 'rb') as f:
        for line in f:
            obj = json.loads(line[0:-1])
            json.dump(obj, out)
            out.write('\n')
            break
    out.close()

    out = codecs.open(output_sample_gtd, 'w')
    with gzip.open(gtd, 'rb') as f:
        for line in f:
            obj = json.loads(line[0:-1])
            json.dump(obj, out)
            out.write('\n')
            break
    out.close()


def acled_extract_types_description(acled=elicit_path+'aditya-3jan-2018/acleddata.jl.gz',acled_out=elicit_path+'aditya-3jan-2018/acled_types_descr.jl'):
    out = codecs.open(acled_out, 'w')
    with gzip.open(acled, 'rb') as f:
        for line in f:
            obj = json.loads(line[0:-1])
            answer = dict()
            answer['id'] = obj['_id']
            answer['types'] = list()
            if 'event_type' in obj['_source'] and len(obj['_source']['event_type']) > 0:
                answer['types'] = obj['_source']['event_type']
            answer['description'] = obj['_source']['event_description']
            json.dump(answer, out)
            out.write('\n')
            # break
    out.close()


def gtd_extract_types_description(gtd=elicit_path+'aditya-3jan-2018/elicit_gtd.jl.gz',gtd_out=elicit_path+'aditya-3jan-2018/gtd_types_descr.jl'):
    out = codecs.open(gtd_out, 'w')
    jsonpath_type = parse('type[*].value')
    jsonpath_description = parse('label[*].value')
    with gzip.open(gtd, 'rb') as f:
        for line in f:
            obj = json.loads(line[0:-1])
            answer = dict()
            answer['id'] = obj['_id']
            answer['types'] = list()
            obj = obj['_source']['knowledge_graph']
            types = [match.value for match in jsonpath_type.find(obj)]
            description = [match.value for match in jsonpath_description.find(obj)]
            if len(description) > 1:
                description = ' '.join(description)
            elif len(description) == 1:
                description = description[0]
            else:
                description = ""
            answer['types'] = types
            answer['description'] = description
            json.dump(answer, out)
            out.write('\n')
            # break
    out.close()


def gtd_extract_from_dist_file(gtd=elicit_path+'gtd_13to16_0617dist.csv', output_file=elicit_path+'gtd-DL-prediction/gtd_info_set.jl'):
    out = codecs.open(output_file, 'w', 'utf-8')
    count = 0
    header_index = dict()
    with codecs.open(gtd, 'rU') as fi:
        f = csv.reader(fi, delimiter=',')
        for fields in f:
            # print count
            if count == 0:
                # print line
                # fields = re.split('\t', line[0:-1])
                for field in range(0, len(fields)):
                    header_index[fields[field]] = field
                count += 1
                continue
                # print firstLine

            # count += 1
            # if count == 1:
            #     continue
            # fields = re.split('\t', line[0:-1])

            eventid = fields[header_index['eventid']]
            year = fields[header_index['iyear']]
            month = fields[header_index['imonth']]
            day = fields[header_index['iday']]
            country_txt = fields[header_index['country_txt']]
            region_txt = fields[header_index['region_txt']]
            location = fields[header_index['location']]
            summary = fields[header_index['summary']]
            attacktype1 = fields[header_index['attacktype1']]
            attacktype1_txt = fields[header_index['attacktype1_txt']]
            targtype1 = fields[header_index['targtype1']]
            targtype1_txt = fields[header_index['targtype1_txt']]

            answer = dict()

            answer['eventid'] = eventid
            answer['year'] = year
            answer['month'] = month
            answer['day'] = day
            answer['country_txt'] = country_txt
            answer['region_txt'] = region_txt
            answer['location'] = location
            answer['summary'] = summary
            answer['attacktype1'] = attacktype1
            answer['attacktype1_txt'] = attacktype1_txt
            answer['targtype1'] = targtype1
            answer['targtype1_txt'] = targtype1_txt
            # print answer
            json.dump(answer, out)
            out.write('\n')

    out.close()

def gtd_serialize_LP_ground_truth_to_edge_list(raw_gt=elicit_path+'gtd-DL-prediction/related-events-gtd.txt',
                                            output_file=elicit_path+'gtd-DL-prediction/LP-gt.tsv'):
    gt_set = set()
    with codecs.open(raw_gt, 'r') as f:
        for line in f:

            if ', ' not in line:
                continue
            else:
                ids = set(re.split('\t',line[0:-1].replace("\"",'').replace(' ','').replace(',','\t')))
                ids.discard('')
                ids = sorted(list(ids))
                for i in range(0, len(ids)-1):
                    for j in range(i+1, len(ids)):
                        gt_set.add(ids[i]+'\t'+ids[j]+'\n')
    pairs = sorted(list(gt_set))
    out = codecs.open(output_file, 'w')
    for p in pairs:
        out.write(p)
    out.close()



# gtd_extract_from_dist_file()
# acled_extract_types_description()
# gtd_extract_types_description()
# extract_nigeria_events_from_gtd()
# gtd_serialize_LP_ground_truth_to_edge_list()

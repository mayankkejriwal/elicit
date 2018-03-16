# import editdistance
import glob
import codecs
import json
from sklearn.feature_extraction.text import *
from scipy import sparse
# import numpy as np
import re
import networkx as nx
from nltk.corpus import stopwords


elicit_path = '/Users/mayankkejriwal/datasets/elicit/constantine-jan29-2018/'

def serialize_entity_types(input_folder=elicit_path+'entity_json/', output_file=elicit_path+'entities-types.json'):
    """
    This function is similar to serialize entities except, instead of strings we are mapping entities to types
    Also, the file is a single json.
    :return:
    """
    answer = dict()
    files = glob.glob(input_folder + '*.json')
    for fi in files:

        obj = json.load(codecs.open(fi, 'r', 'utf-8'))

        if 'entities' not in obj or len(obj['entities']) == 0:
            continue


        for entity in obj['entities']:
            answer[entity['id']] = entity['entityType']

    json.dump(answer, codecs.open(output_file, 'w'),indent=4)

def serialize_entities(input_folder=elicit_path+'entity_json/', output_file=elicit_path+'entities.jsonl'):
    files = glob.glob(input_folder + '*.json')
    out = codecs.open(output_file, 'w', 'utf-8')
    stopWords = set(stopwords.words('english'))
    for fi in files:

        obj = json.load(codecs.open(fi, 'r', 'utf-8'))

        if 'entities' not in obj or len(obj['entities']) == 0:
            continue

        doc_id = obj['id'] # only to debug. Entity ids seem to be corpus-unique.
        answer = dict()
        inner_dict = dict()
        for entity in obj['entities']:
            mentionTokens = list()
            if 'mentions' not in entity:
                continue
            for mention in entity['mentions']:
                if 'mentionType' not in mention or mention['mentionType'] != 'name':
                    continue
                if 'headTokens' in mention:
                    mentionTokens += mention['headTokens']
                elif 'tokens' in mention:
                    mentionTokens += mention['tokens']
            if len(mentionTokens) > 0:
                new_list = list()
                import string
                for m in mentionTokens:
                    if m.lower() in stopWords or m.lower() in string.punctuation:
                        continue
                    new_list.append(m.lower())
                if len(new_list) == 0:
                    continue
                new_list = list(set(new_list))
                new_list.sort()
                inner_dict[entity['id']] = new_list # first pass: tokens are order-ind.
        if inner_dict:
            answer[doc_id] = inner_dict
            json.dump(answer, out)
            out.write('\n')

    out.close()


def level1_clustering(serialized_file=elicit_path+'entities.jsonl', level1_clustering=elicit_path+'entities-level1.jsonl'):
    """
    cluster defined by duplicate-token-removal+sorting+exact match

    We'll assume entity ids are corpus-unique, across as well as within documents. Haven't found any violations.
    :param serialized_file:
    :param level1_clustering:
    :return:
    """
    string_entity_dict = dict()
    with codecs.open(serialized_file, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line[0:-1])
            entities = obj[obj.keys()[0]]
            for k, v in entities.items():
                # vlower = [v_el.lower() for v_el in v]
                # vlower.sort()
                token_string = ' '.join(v)
                if token_string not in string_entity_dict:
                    string_entity_dict[token_string] = list()
                string_entity_dict[token_string].append(k)
                # break
            # break
    out = codecs.open(level1_clustering, 'w', 'utf-8')
    for k, v in string_entity_dict.items():
        answer = dict()
        answer[k] =sorted(v)
        json.dump(answer, out)
        out.write('\n')
    out.close()

def _space_splitter(text):
    return re.split(' ',text)

def tfidf_level1_entities(level1_clustering=elicit_path+'entities-level1.jsonl', tfidf_output_thresh=elicit_path+'entities-tfidf-thresh-0.8.jsonl',
                          threshold=0.8):
    """
    tokens-tfidf on level 1 entities
    :param level1_clustering:
    :param level2_clustering:
    :return:
    """
    raw_data = list()
    with codecs.open(level1_clustering, 'r', 'utf-8') as f:
        for line in f:
            entity = json.loads(line[0:-1]).keys()[0]
            raw_data.append(entity)
    print 'number of unique string mention entities...',str(len(raw_data))
    tfidf = TfidfVectorizer(analyzer='word', tokenizer=_space_splitter)
    entity_term_matrix = tfidf.fit_transform(raw_data)
    entity_entity_sims = entity_term_matrix.dot(sparse.csr_matrix.transpose(entity_term_matrix))

    print 'finished computing entity_entity_sims...'
    out = codecs.open(tfidf_output_thresh, 'w', 'utf-8')
    for i in range(0, entity_entity_sims.shape[0]):
        dat = entity_entity_sims[i].data
        ind = entity_entity_sims[i].indices
        # print ind[0]
        # print ind[1]
        # break
        answer = dict()
        answer[raw_data[i]] = dict()
        item = False
        for j in range(0,len(ind)):
            if ind[j] == i:
                continue
            if dat[j] > threshold:
                item = True
                answer[raw_data[i]][raw_data[ind[j]]] = dat[j]
        if item:
            json.dump(answer, out)
            out.write('\n')
    out.close()


def serialize_tfidf_as_edge_list(tfidf_output_file=elicit_path+'entities-tfidf-thresh-0.8.jsonl',
                                 edge_list = elicit_path+'entities-tfidf-thresh-0.8-edge-list.tsv'):
    """
    the edges are really not complete and that's why the connected components are so important.
    :param tfidf_output_file:
    :param edge_list:
    :return:
    """
    out = codecs.open(edge_list, 'w', 'utf-8')
    with codecs.open(tfidf_output_file, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line[0:-1])
            entity = obj.keys()[0]
            if '\t' in entity:
                raise Exception
            for k in obj[entity].keys():
                if '\t' in k:
                    raise Exception
                else:
                    out.write(entity+'\t'+k+'\n')

    out.close()


def connected_components_on_edge_list(edge_list = elicit_path+'entities-tfidf-thresh-0.8-edge-list.tsv',
                                      components = elicit_path+'conn-components-tfidf-thresh-0.8-edge-list.jsonl'):

    G = nx.read_edgelist(edge_list, delimiter='\t')
    out = codecs.open(components, 'w', 'utf-8')
    print 'is the graph directed? ',
    print G.is_directed()
    print 'num connected components...', str(nx.number_connected_components(G))
    conn_comps = sorted(nx.connected_components(G), key=len)
    # print type(conn_comps)
    # singleton_conn_comp = 0
    for c in conn_comps:
        json.dump(list(c), out)
        out.write('\n')

    out.close()

def serialize_clusters(clusters = elicit_path+'conn-components-tfidf-thresh-0.8-edge-list.jsonl', entities_level1_file=elicit_path+'entities-level1.jsonl',
                       serialized_clusters = elicit_path+'SERIALIZED-conn-components-tfidf-thresh-0.8-edge-list.jsonl',
                       entities_types=elicit_path+'entities-types.json') :
    string_entity_dict = dict()
    blacklist = set([])
    entity_type_dict = json.load(codecs.open(entities_types, 'r'))
    # cluster_entity_dict = dict()
    with codecs.open(entities_level1_file, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line[0:-1])
            string = obj.keys()[0]
            string_entity_dict[string] = obj[string]

    count = 0
    out = codecs.open(serialized_clusters, 'w', 'utf-8')
    with codecs.open(clusters, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line[0:-1])
            entities = list()
            # strings = dict()
            type_entity_dict = dict()
            type_string_dict = dict()
            for k in obj:
                if k in blacklist:
                    continue
                if k not in string_entity_dict:
                    raise Exception
                else:
                    for entity in string_entity_dict[k]:
                        t = entity_type_dict[entity]
                        if t not in type_entity_dict:
                            type_entity_dict[t] = list()
                        type_entity_dict[t].append(entity)
                        if t not in type_string_dict:
                            type_string_dict[t] = dict()
                        if k not in type_string_dict[t]:

                            type_string_dict[t][k] = 0
                        type_string_dict[t][k] += 1
                        # if k not in strings:
                        #     strings[k] = 0
                        # strings[k] += len(string_entity_dict[k])
            # cluster_id = str(count)




            for t, entity in type_entity_dict.items():
                answer = dict()
                answer["id"]=count
                count += 1
                answer['entities'] = entity
                string_dict = type_string_dict[t]
                answer['strings'] = string_dict
                json.dump(answer, out)
                out.write('\n')

    out.close()


# serialize_entity_types()
# serialize_entities()
# level1_clustering()
# tfidf_level1_entities()
# serialize_tfidf_as_edge_list()
# connected_components_on_edge_list()
serialize_clusters()
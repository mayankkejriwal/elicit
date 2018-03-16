from sklearn.feature_extraction.text import *
import json, codecs
import re
import nltk
from scipy import sparse
import numpy as np

elicit_path='/Users/mayankkejriwal/datasets/elicit/aditya-3jan-2018/'


def tokenizer(text):
    return [word.lower() for sent in nltk.sent_tokenize(text) for word in nltk.word_tokenize(sent)]

def _try_tfidfVectorizer():
    raw_strings1 = ['I am a man','I am a girl','The girl and the man are friends']
    k1 = TfidfVectorizer(analyzer='word', tokenizer=tokenizer)
    p1 = k1.fit_transform(raw_strings1)
    print p1.shape
    m = p1[0:2]
    print m
    n = sparse.csr_matrix.transpose(p1[2])
    print n
    l = m.dot(n)
    print
    print l
    print l[0].data
    print l[0].indices
    # print p[0].indices
    # print k.get_feature_names()


def docs_to_tfidf(acled=elicit_path+'acled_types_descr.jl',gtd=elicit_path+'gtd_types_descr.jl',
                  acled_tfidf_matrix_out=elicit_path+'acled_gtd_description_tfidf_matrix.jl',
                  gtd_tfidf_matrix_out=elicit_path + 'gtd_acled_description_tfidf_matrix.jl',
                  write_acled=False):
    """

    :param acled:
    :param gtd:
    :param acled_tfidf_matrix_out:
    :param gtd_tfidf_matrix_out:
    :param write_acled: if True then we compute dot product between acled and gtd, otherwise we compute
    dot product between gtd and acled.
    :return:
    """
    raw_data = list()
    count = 0
    with codecs.open(acled, 'r') as f:
        for line in f:
            obj = json.loads(line[0:-1])
            raw_data.append(obj['description'])
            count += 1

    acled_begin = 0
    acled_end = count
    gtd_begin = count

    with codecs.open(gtd, 'r') as f:
        for line in f:
            obj = json.loads(line[0:-1])
            raw_data.append(obj['description'])
            count += 1

    gtd_end = count

    # print acled_begin
    # print acled_end
    # print gtd_begin
    # print gtd_end

    print 'finished reading in the data...'

    tfidf = TfidfVectorizer(analyzer='word', tokenizer=tokenizer)
    doc_term_matrix = tfidf.fit_transform(raw_data)

    print 'finished building doc_term_matrix...'



    if write_acled is True:
        dt_acled = doc_term_matrix[acled_begin:acled_end]
        dt_gtd = sparse.csr_matrix.transpose(doc_term_matrix[gtd_begin:gtd_end])
        tfidf_matrix = dt_acled.dot(dt_gtd)
        print 'finished tfidf matrix between acled and gtd, printing shape...'
        print tfidf_matrix.shape
        out = codecs.open(acled_tfidf_matrix_out, 'w')

        for i in range(0, tfidf_matrix.shape[0]):
            dat = tfidf_matrix[i].data
            ind = tfidf_matrix[i].indices
            answer = dict()
            answer['acled_int_id'] = i
            answer['gtd_scores'] = dict()
            for k in range(0, len(ind)):
                answer['gtd_scores'][str(ind[k])] = dat[k]
            json.dump(answer, out)
            out.write('\n')

        out.close()
    else:
        dt_acled = sparse.csr_matrix.transpose(doc_term_matrix[acled_begin:acled_end])
        dt_gtd = doc_term_matrix[gtd_begin:gtd_end]
        tfidf_matrix = dt_gtd.dot(dt_acled)
        print 'finished tfidf matrix between gtd and acled, printing shape...'
        print tfidf_matrix.shape
        out = codecs.open(gtd_tfidf_matrix_out, 'w')
        # tfidf_matrix = sparse.csr_matrix.transpose(tfidf_matrix)
        for i in range(0, tfidf_matrix.shape[0]):
            dat = tfidf_matrix[i].data
            ind = tfidf_matrix[i].indices
            answer = dict()
            answer['gtd_int_id'] = i
            answer['acled_scores'] = dict()
            for k in range(0, len(ind)):
                answer['acled_scores'][str(ind[k])] = dat[k]
            json.dump(answer, out)
            out.write('\n')

        out.close()

    # print doc_term_matrix.shape

def postprocess_doc_to_tfidf_outputs(acled_results=elicit_path+'acled_gtd_description_tfidf_matrix.jl',acled_file=elicit_path+'acled_types_descr.jl',
                                 gtd_results=elicit_path + 'gtd_acled_description_tfidf_matrix.jl',
                                 gtd_file=elicit_path + 'gtd_types_descr.jl', topk=1, output_gtd_acled=elicit_path + 'gtd_acled_0.3.tsv'):
    acled = dict()
    gtd = dict()
    count = 0
    with codecs.open(acled_file, 'r') as f:
        for line in f:
            obj = json.loads(line[0:-1])
            acled[count] = obj
            count += 1
    count = 0
    with codecs.open(gtd_file, 'r') as f:
        for line in f:
            obj = json.loads(line[0:-1])
            gtd[count] = obj
            count += 1
    counter = 0

    # with codecs.open(acled_results, 'r') as f:
    #     for line in f:
    #         indices = list()
    #         data = list()
    #         obj = json.loads(line[0:-1])
    #         for k,v in obj['gtd_scores'].items():
    #             indices.append(int(k))
    #             data.append(float(v))
    #         # print data
    #         # topk_indices = get_topk_indices(data, topk)
    #         topk_indices = sorted(range(len(data)), key=lambda i: data[i], reverse=True)[:topk]
    #         # print topk_indices
    #         print 'ACLED DESCRIPTION'
    #         print acled[obj['acled_int_id']]['description']
    #         print 'GTD DESCRIPTIONS'
    #         for ind in topk_indices:
    #             print gtd[indices[ind]]['description']
    #         if counter == 5:
    #             break
    #         else:
    #             counter += 1

    # counter = 0
    # gtd_scores = list()
    out = codecs.open(output_gtd_acled, 'w')
    with codecs.open(gtd_results, 'r') as f:
        for line in f:
            indices = list()
            data = list()
            obj = json.loads(line[0:-1])
            for k, v in obj['acled_scores'].items():
                indices.append(int(k))
                data.append(float(v))
            # print data
            # topk_indices = get_topk_indices(data, topk)
            topk_indices = sorted(range(len(data)), key=lambda i: data[i], reverse=True)[:topk]
            if len(topk_indices) == 0 or data[topk_indices[0]] < 0.3:
                continue
            else:
                out.write(gtd[obj['gtd_int_id']]['id']+'\t'+acled[indices[topk_indices[0]]]['id']+'\n')
            # print topk_indices[0]
            # print topk_indices
            # print len(data)
            # break

            # print 'GTD DESCRIPTION'
            # print gtd[obj['gtd_int_id']]['description']
            # print 'ACLED DESCRIPTIONS'
            # for ind in topk_indices:
            #     print acled[indices[ind]]['description']
            # if counter == 5:
            #     break
            # else:
            #     counter += 1
    # print np.mean(gtd_scores)
    # print np.std(gtd_scores)
    out.close()


@DeprecationWarning
def get_topk_indices(data, k):
    if len(data) <= k:
        print 'warning. length of data ',str(len(data)), ' is smaller than k ',str(k)
        return range(0,len(data))
    else:
        new_data = list(data)
        p = sorted(new_data, reverse=True)[0:k]
        answer = list()
        for element in p:
            i = new_data.index(element)
            answer.append(i)
            new_data[i] = 0 # this is important, so we don't repeat indices
        if len(set(answer)) != len(answer):
            raise Exception
        else:
            return answer


postprocess_doc_to_tfidf_outputs()
# a=[1.0, 9.1, 8.0, -3]
# p = sorted(range(len(a)), key=lambda i: a[i], reverse=True)[:2]
# print p
# docs_to_tfidf()
# _try_tfidfVectorizer()
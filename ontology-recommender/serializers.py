import codecs, re, json, gzip
import csv
import glob
import networkx as nx
from networkx.drawing import nx_agraph


elicit_path='/Users/mayankkejriwal/datasets/elicit/'


def read_elicit_file(input_file=elicit_path+'elicit.jl.gz'):
    with gzip.open(input_file, 'r', 'utf-8') as f:
        for line in f:
            obj = json.loads(line[0:-1])
            # print json.dumps(obj, indent=4, sort_keys=True)
            print json.dumps(obj)
            break


def read_gtd_csv_file(input_file=elicit_path+'gtd_13to16_0617dist-2.csv'):
    gtd_list = list()
    with open(input_file, 'rb') as csvfile:
        rows = csv.reader(csvfile, delimiter=',')
        for row in rows:
            gtd_list.append(row)

    print len(gtd_list)


def get_concept_hierarchy(input_directory=elicit_path+'pedro-ontologies-dec7-2017/',
                          output_file=elicit_path+'pedro-ontologies-dec7-2017.directed_edge_list'):
    """
    Directed edge list in format
    :param input_directory:
    :return:
    """
    files = glob.glob(input_directory + '*.jl')
    print files
    lines = set()
    for fi in files:
        print 'processing file...',fi
        with codecs.open(fi, 'r', 'utf-8') as f:
            for line in f:
                obj = json.loads(line[0:-1])
                if '@type' in obj and 'owl:Class' in obj['@type'] and 'rdfs:subClassOf' in obj:

                    superclasses = obj['rdfs:subClassOf']
                    this_class = obj['@id']
                    for s in superclasses:
                        lines.add(s+'\t'+this_class)
                else:
                    continue
    print 'number of unique super-sub statements: ',len(lines)
    out = codecs.open(output_file, 'w', 'utf-8')
    lines = list(lines)
    lines.sort()
    for l in lines:
        out.write(l+'\n')
    out.close()


def serialize_edge_list_to_graphviz_dot(edge_list=elicit_path + 'pedro-ontologies-dec7-2017.directed_edge_list',
                                        output_file=elicit_path + 'pedro-ontologies-dec7-2017-directed.dot', directed=True):
    if directed is True:
        G = nx.read_edgelist(edge_list, delimiter='\t', create_using=nx.DiGraph())
    else:
        G = nx.read_edgelist(edge_list, delimiter='\t')
    nx_agraph.write_dot(G, output_file)


# serialize_edge_list_to_graphviz_dot()
# get_concept_hierarchy()
# read_elicit_file()
# read_gtd_csv_file()
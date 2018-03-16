import demjson
import sys
import os
import re
import hashlib

cco = '<http://www.ontologyrepository.com/CommonCoreOntologies/'
sosa = '<http://www.w3.org/ns/sosa'
prov = '<http://www.w3.org/ns/prov'
ce = '<http://ontology.causeex.com/ontology/odps/CauseEffect'
dprov = '<http://ontology.causeex.com/ontology/odps/DataProvenance#'

onts = {
    "type": '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>',
    "label": '<http://www.w3.org/2000/01/rdf-schema#label>',

    "measurements": cco + 'MeasurementInformationContentEntity>',
    'infobearing': cco + 'InformationBearingEntity>',
    'activity': dprov + 'Activity>',

    'is_a_measurement_of': cco + 'is_a_measurement_of>',
    'is_measured_by': cco + 'is_measured_by>',
    'uses_measurement_unit': cco + 'uses_measurement_unit>',
    'describes': cco + 'describes>',
    'has_capability': cco + 'has_capability>',
    'designates': cco + 'designates>',
    "designated_by": cco + 'designated_by>',
    "locin": cco + 'LewisianRelationOntology/located_in>',

    'gpe': cco + 'CommonCoreOntologies/GeopoliticalEntity>',
    'state': cco + 'State>',
    'country': cco + 'Country>',

    'observation': sosa + '/Observation>',
    'simple_result': sosa + '/hasSimpleResult>',
    'time': sosa + '/resultTime>',
    'feature': sosa + '/hasFeatureOfInterest>',

    'primarysource': prov + '#hadPrimarySource>',
    'generated_by': dprov + 'generated_by>',

    'has_cause': ce + '#has_cause>',
    'has_effect': ce + '#has_effect>',
    'causalassertion': ce + '#CausalAssertion>',
}


def make_uid(ent):
    return hashlib.sha256(ent).hexdigest().upper()


def get_uri(id, suffix):
    return '<http://elicit.isi.edu/data/{}/{}>'.format(id, suffix)


def get_obs_uri(id):
    return get_uri(id, 'observation')


def get_ca_uri(id):
    return get_uri(id, 'causal_assertion')


def get_foi_uri(id):
    return get_uri(id, 'foi')


def as_date(txt):
    return '{}^^<http://www.w3.org/2001/XMLSchema#date>'.format(txt)


def as_double(txt):
    return '{}^^<http://www.w3.org/2001/XMLSchema#double>'.format(txt)


def print_triple(s, p, o):
    print '{}\t{}\t{} .'.format(s, p, o)


def quote_string(txt):
    txt = re.sub(r'\/', r'', str(txt))
    txt = re.sub(r'\n', r' ', str(txt))
    return '"{}"'.format(txt)


mapping = sys.argv[2]
file = sys.argv[1]
fh = open(file, 'rb')
mfh = open(mapping, 'rb')
var_to_uri = dict()
sources = dict()

for line in mfh:
    parts = line.split("\t")
    var_to_uri[parts[1].strip()] = '<' + parts[0].strip() + '>';

for line in fh:
    parts = line.split("\t")
    source = parts[0]
    cause_var = parts[1].strip()
    effect_var = parts[2].strip()
    if source not in sources:
        sources[source] = get_uri(make_uid(source), 'source')
        print_triple(sources[source], onts['type'], onts['activity'])
        print_triple(sources[source], onts['label'], quote_string(source))

    ca_id = get_ca_uri(make_uid(cause_var + ":" + effect_var + ":" + source))
    print_triple(ca_id, onts['type'], onts['causalassertion'])
    print_triple(ca_id, onts['has_cause'], var_to_uri[cause_var])
    print_triple(ca_id, onts['has_effect'], var_to_uri[effect_var])
    print_triple(ca_id, onts['primarysource'], sources[source])

fh.close()

import json
import operator
import sys
from typing import Dict

from attr import attrs, attrib

elicit_path = '/Users/mayankkejriwal/datasets/elicit/constantine-jan29-2018/'

@attrs(slots=True)
class Entity:
    id: int = attrib()
    entities: int = attrib()
    strings: Dict[str, int] = attrib()


entities = []
for line in open(elicit_path+'output-0.8.jsonl'):
    line_json = json.loads(line)
    entities.append(Entity(line_json['id'], len(line_json['entities']), line_json['strings']))
sorted_entities = sorted(entities, key=operator.attrgetter('entities'), reverse=True)

print('\t'.join(['id', 'n_entities', 'strings']))
for entity in sorted_entities:
    sorted_strings = sorted(entity.strings.items(), key=operator.itemgetter(1), reverse=True)
    formatted_strings = ['{!r}({})'.format(*item) for item in sorted_strings]
    print('\t'.join([str(entity.id), str(entity.entities), ' '.join(formatted_strings)]))
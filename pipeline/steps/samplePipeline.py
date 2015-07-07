import sys
import json
from copy import deepcopy


def run(entry):

    """
    Sample Python pipeline step. Searches for "Voyager" or "voyager" and returns the word count.
    :param entry: a JSON file containing a voyager entry.
    """
    orig_entry = json.load(open(entry, "rb"))
    new_entry = deepcopy(orig_entry)
    voyager_word_count = 0
    if 'fields' in orig_entry and 'text' in orig_entry['fields']:
        text_field = orig_entry['fields']['text']
        voyager_word_count += text_field.Count('Voyager')
        voyager_word_count += text_field.Count('voyager')
        new_entry['fields']['fi_voyager_word_count'] = voyager_word_count
    sys.stdout.write(json.dumps(new_entry))
    sys.stdout.flush()

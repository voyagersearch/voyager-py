import sys
import json


def run(entry):
    """
    Sample Python pipeline step. Searches the text field for "Voyager" or "voyager" and returns the word count.
    :param entry: a JSON file containing a voyager entry.
    """
    new_entry = json.load(open(entry, "rb"))
    voyager_word_count = 0
    if 'fields' in new_entry['entry']:
        if 'text' in new_entry['entry']['fields']:
            text_field = new_entry['entry']['fields']['text']
            voyager_word_count += text_field.count('Voyager')
            voyager_word_count += text_field.count('voyager')
            new_entry['entry']['fields']['fi_voyager_word_count'] = voyager_word_count
    sys.stdout.write(json.dumps(new_entry))
    sys.stdout.flush()

####Follow these steps to translate the messages used in the Voyager Search Georpcoessing tasks

1. Make a copy of any messages_*.po file in the LC_MESSAGES folder and rename it to to reflect the new language (e.g. messages_De.po, messages_Fr.po, etc.). **Use only two letters**
2. Edit the new .po file and update the "msgstr" fields with the translated strings. {0} represents a positional argument in Python and depending on the language translation, this may require being positioned in a different location in the string
4. Complete all the translations and save the .po file

The first time a task is executed, the PO file will be compiled to a MO file (e.g. messages_De.mo). This new binary file is used
to look up the translated messages. If you need to generate the MO file without executing a task, run the *make_mo_files.py* located in the locale folder.




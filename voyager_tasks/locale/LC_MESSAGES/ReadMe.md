####Follow these steps to translate the messages used in the Voyager Search Georpcoessing tasks

1. Make a copy of messages.pot in the LC_MESSAGES folder and rename it to to reflect the new language (e.g. messages_De.po, messages_Fr.po, etc.). **Use only two letters**
2. Edit the new .po file and update the "msgstr" fields with the translated strings. {0} represents a positional argument in Python and depending on the language translation, this may require being positioned in a different location in the string
4. Complete all the translations and save the .po file

The first time a task is executed, the .po file will be compiled to a .mo file (e.g. messages_De.mo). This new binary file is used
to look up the translated messages. If you want generate the .mo file without executing a task, run the *msgfmt.py* script located in the tools/i18n folder within your Python install location:

    msgfmt.py -o LC_MESSAGES/messages_De.mo LC_MESSAGES/messages_De.po



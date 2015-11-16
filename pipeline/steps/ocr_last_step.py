import sys, json, shutil, os, time

from PIL import *
from pytesser import *

import PythonMagick

ocrFileTypes = [".bmp", ".gif", ".jpg", ".png", ".tif"]
ocrstatus = "Init"

def run(entry):

  vdoc = json.load(open(entry, "rb"))

  ## UNCOMMENT TO WRITE ENTRY JSON FOR DEBUGGING
  ## with open("c:/temp/entry.json","wb") as fp:
  ##  json.dump(vdoc, fp)

  if 'fields' in vdoc['entry']:

    ocrstatus = "PREAMBLE - Have Fields"
    ocrpath = "nopath"
    ocrextn = "noextn"
    ocrtext = "notext"

    if vdoc['job']:

      if 'path' in vdoc['job']:

        ocrstatus = ocrstatus + ",PREAMBLE - Have Job & Path"
        ocrpath = vdoc['job']['path']
        ocrextn = os.path.splitext(ocrpath)[1].lower()

        ### test if an extension is a PDF and convert accordingly ...

        if ocrextn == ".pdf":

          try:

            ts = str(time.time())
            ts = r"C:\temp\{0}.png".format(ts)
            ocrstatus = ocrstatus + ", Converting PDF to {0}".format(ts)

            img = PythonMagick.Image()
            img.density("300")
            img.read(r"{0}".format(ocrpath))
            img.write(ts)
            ocrtext = image_file_to_string(ts, graceful_errors=True)
            ocrstatus = ocrstatus + ", SUCCESS OCR'ing Converted PDF"

            #try:
              #os.remove(ts)
            #except Exception as ex:
              #pass

          except Exception as ex:

            ocrstatus = ocrstatus + ", EXCEPTION Converting / OCR'ing PDF"
            ocrtext = "OCR FAILED :: {0}".format(ex.message)
            print ocrtext

        if ocrextn in ocrFileTypes:

          ocrstatus = "OCR on {0} ({1}) - ".format(ocrpath, ocrextn)

          try:

            ocrtext = image_file_to_string(ocrpath, graceful_errors=True)
            ocrstatus = ocrstatus + ", SUCCESS OCR'ing IMG"

          except Exception as ex:

            ocrtext = "OCR FAILED :: {0}".format(ex.message)
            ocrstatus = ocrstatus + ", EXCEPTION OCR'ing IMG"
            pass

        elif ocrextn != ".pdf":

          ocrtext = "{0} is not an OCR compatible file type".format(ocrextn)
          ocrstatus = ocrstatus + ", INCOMPATIBLE"

    vdoc['entry']['fields']['fs_ocrstatus'] = ocrstatus
    vdoc['entry']['fields']['fs_ocrpath'] = ocrpath
    vdoc['entry']['fields']['fs_ocrextn'] = ocrextn
    vdoc['entry']['fields']['fs_ocrtext'] = ocrtext

  sys.stdout.write(json.dumps(vdoc))
  sys.stdout.flush()

## UNCOMMENT TO READ ENTRY JSON FOR DEBUGGING (Run from PyScripter)
##if __name__ == '__main__':
  ##entry_file = "c:/temp/entry.json"
  ##run(entry_file)
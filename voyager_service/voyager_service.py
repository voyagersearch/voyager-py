import os, imp
from bottle import Bottle, route, run

ROOT_APP = Bottle()

@ROOT_APP.route('/')
def root_index():
    return 'Voyager Service Root'

if __name__ == '__main__':

    ALL_SERVICES = os.listdir('services')
    # what else might need to be removed?
    ALL_SERVICES.remove('__init__.py')
    for service_name in ALL_SERVICES:
        # does it end in .py? can add additional checking here.
        # maybe enforce a "name.service.py" convention.
        if service_name.split('.')[-1] == 'py':
            foo = imp.load_source('module', 'services{0}{1}'.format(os.sep, service_name))
            try:
                ROOT_APP.merge(foo.service)
            except Exception as e:
                print 'error while merging routes from {0}: {1}'.format(service_name, e)

    print "\nVALID APP ROUTES: \n"
    print ROOT_APP.routes

    ROOT_APP.run(debug=True, host='localhost', port=9998)


'''
    todo: 
            reload dynamically, maybe when new file is added
            use filename for prefix
            pass address / port into pipeline steps 
            ability to have multiple directories of services

            generify nlp interface to use services in here 
            can we stop single services? 
            run from voyager process 
            add flag to "not start" service 
            async loading? 
            licensing? maybe have them be inactive until needed
            ability to specify / switch out engine - cherrypy, bjoern 

            add config to use bottle, cherrypy, bjoern 
            turn off service, specify url, voyager passes that to pipeline steps 
            whitelist / blacklist to exclude scripts 

            same format as NLP plugin 
            put in voyager-py 
            add code to run service, sample services, new root 
            pass config through pipeline steps 

            changes for NLP plugin to use this service 

            how to expose in manage ui?


'''
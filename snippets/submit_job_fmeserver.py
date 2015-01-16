# (C) Copyright 2014 Voyager Search
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import sys
import json
import urllib
import urllib2


def send_job(in_features, clip_features, out_workspace, out_coordinate_sys):
    """
    Submits a job to FME Server.

    :param in_features: Input features to be clipped (i.e. input shapefile)
    :param clip_features: Features used for clipping the input
    :param out_workspace: The location where the output features are created
    :param out_coordinate_sys: The ID for the coordinate system (i.e.  LL-WGS84)

    """
    # FME Service URL
    url = "http://localhost/fmejobsubmitter/Samples/ClipDataFME.fmw"

    # This is just an example assuming a service with these published parameters.
    # The keys are the FME service parameter names.
    params = {
        "Clippee": in_features,
        "Clipper": clip_features,
        "DestDataset": out_workspace,
        "OutCoordSys": out_coordinate_sys,
        "opt_showresult": "true",
        "opt_responseformat": "json"  # Response format will be json
    }

    # Create and post the request.
    try:
        password_mgr = urllib2.HTTPPasswordMgrWithDefaultRealm()
        password_mgr.add_password(None, url, 'user', 'pass')
        urllib2.install_opener(urllib2.build_opener(urllib2.HTTPBasicAuthHandler(password_mgr)))
        parameters = urllib.urlencode(params)
        response = urllib2.urlopen(url, parameters)
        if response.code == 200:  # Success
            response_dict = json.loads(response.read())
            srv_response = response_dict['serviceResponse']
            # Can ask the response for things like number of features, etc.
            if srv_response['fmeTransformationResult']['fmeEngineResponse']['numFeaturesOutput'] > 0:
                print('Clipped: {0}').format(in_features)
            else:
                print('No features were clipped.')
        else:
            print ('Error: {0}'.format(response.code))
    except urllib2.HTTPError as http_error:
        print(http_error)
    except urllib2.URLError as url_error:
        print(url_error)
# End send_job function


if __name__ == '__main__':
    # Set up the arguments - can be run standalone or via the command line.
    input_features = sys.argv[1] or "C:/GISData/Shapefiles/cities.shp"
    clip_features = sys.argv[2] or "C:/GISData/ShapeFiles/states.shp"
    output_workspace = sys.argv[3] or "C:/GISData/Shapefiles/clip_results"
    output_coordinate_system = sys.argv[4] or "LL-WGS84"
    send_job(input_features, clip_features, output_workspace, output_coordinate_system)

![Voyager Logo](https://voyagersearch.com/static/img/voyagerlogo.png) 


Voyager Python
=====

# voyager-py

This repository includes the Python source code for the Voyager Search processing tasks, location workers and extractors.
  
  
## processing
* The processing framework for Voyager Search is written entirely in Python. Tasks are geoprocessing operations that can be run on search results.
* For more information about using the processing tasks, refer to the documentation located here,
 [Processing](https://voyagersearch.zendesk.com/hc/en-us/sections/200577153-Processing)
* For more information about creating custom tasks, refer to the developer documentation located here, 
[Create a new processing task](https://voyagersearch.zendesk.com/hc/en-us/articles/203569408-Creating-a-New-Processing-Task)


## locations
* In version 1.9.4, Voyager Search added support for indexing databases using Python, 
allowing users to create a new custom location that permits the indexing of multiple tables or collections at one time. 
Each record in each table becomes an item indexed in Voyager Search. For more information and examples for indexing locations, refer to the documentation located here,
[Indexing Tables](https://voyagersearch.zendesk.com/hc/en-us/sections/200495997-Indexing-Tables).
* These workers are developed entirely in Python and can be extended to support additional database types.


## extractors
* These are Python based extractors for reading the properties of data and indexing that information. This project is in development and only one sample extractor is 
  currently available here. 


## Instructions

1. Clone the repo.

 [New to Github? Get started here.](https://github.com/)

## Requirements

* Voyager Search - Voyager will install the following 32bit libraries: GDAL, pyodbc, pymongo, pyzmq, cx_Oracle and requests.
* For most processing tasks, ArcGIS is required (preferably ArcGIS 10.1 or higher). ArcGIS will include the arcpy Python library.
* Python 2.6 is installed and required for ArcGIS 10.0.
* Python 2.7 is installed and required for ArcGIS 10.1, 10.2 and 10.3.
* Python 3.x can be used but will not work with ArcGIS.


## Resources

* [Processing Tasks](https://voyagersearch.zendesk.com/hc/en-us/sections/200577153-Processing)
* [Create a new processing task](https://voyagersearch.zendesk.com/hc/en-us/articles/203569408-Creating-a-New-Processing-Task)
* [Indexing Tables](https://voyagersearch.zendesk.com/hc/en-us/sections/200495997-Indexing-Tables)

## Issues

Find a bug or want to request a new feature?  Please let us know by submitting an issue.


## Licensing
(C) Copyright 2014 Voyager Search

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

A copy of the license is available in the repository's [license.txt](https://github.com/voyagersearch/voyager-py/blob/master/LICENSE.txt) file.

   
----

Build: @BUILD_NUMBER@


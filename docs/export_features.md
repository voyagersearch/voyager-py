#Export Features

Exports indexed tabular data (features) to a new shapefile or file geodatabase.

###Usage Tips
  - The supported output formats are Esri Shapefile and Esri File Geodatabase.
  - Tabular data with geographic information are exported as new shapefiles or as feature classes in a file geodatabase.
  - Search results without geographic information will not be exported.
  - The outputs are zipped and can be downloaded.
  - For each unique location, a new folder (for shapefiles) or file geodatabase is created.
  - For each unique title, a new shapefile or feature class is created with that name. 
  - When exporting to Shapefiles, ArcGIS is not required.
  - When exporting to a Geodatabase, ArcGIS is required.
  - If search results are polygon or polyline features without Well-Known Text (WKT) information, the bounding box or extent of that feature is exported.

####Further Discussion
This task is intended to export features that have been indexed from a database table or 
vector file such as a shapefile or AutoCAD drawing.

Click [here](https://voyagersearch.zendesk.com/hc/en-us/articles/204199987-Python-Indexing-Overview) to learn more about indexing databases or vector data formats using Python.

Every indexed item contains a property for location and title. A new folder or file geodatabase is created for each unique location and a new shapefile or feature class is created for each unique title.
For example, when exporting to shapefiles, an item with a location value of "USA" and a title value of "ROADS" outputs a shapefile named ROADS in a folder named USA.
When exporting to a file geodatabase, a result with a location value of "USA" and a title value of "ROADS results in a new feature class named ROADS with a file geodatabase named USA.
All items with the same location and title are added to the same output if it already exists.

[Voyager Search]:http://voyagersearch.com/
[@VoyagerGIS]:https://twitter.com/voyagergis
[github]:https://github.com/voyagersearch/tasks

    

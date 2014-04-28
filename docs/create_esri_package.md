#Create an Esri Map or Layer Package

Package selected search results into a single compressed file (.mpk or .lpk).

###Usage Tips
  - A processing extent can be specified to limit the geographic area being packaged.
  - If no processing extent is specified, the full extent of inputs is used.
  - The projection is used to project the output results. The default is WGS84.
  - The package file can be downloaded and opened directly in ArcMap.
  - The output data for layer and map packages is a file geodatabase.
  - Non-spatial files such as text files, PDF, and Office documents will be included in the package.
  - Although optional, it's recommended to provide a summary and tags as this will make searching for the package easier.
  - Tags can be separated using commas or semi-colons.

###Screen Captures
![Create Esri Package] (imgs/create_esri_package_0.png "Create Esri Package example")

###Requirements
    - ArcGIS 10.x

### See Also
[ArcGIS tool for packaging maps]: http://resources.arcgis.com/en/help/main/10.2/#/Package_Map/0017000000q5000000/ "Package Map"
[ArcGIS tool for packaging layers]: http://resources.arcgis.com/en/help/main/10.2/#/Package_Layer/0017000000q4000000/ "Package Layer"
- Learn more about the [ArcGIS tool for packaging maps] used by this task
- Learn more about the [ArcGIS tool for packaging layers] used by this task


[Voyager Search]:http://voyagersearch.com/
[@VoyagerGIS]:https://twitter.com/voyagergis
[github]:https://github.com/voyagersearch/tasks


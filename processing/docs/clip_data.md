#Clip Data

Clips selected search results using the clip geometry. 

###Usage Tips
  - The clip geometry can be specified as a rectangle or polygon. For rasters, the extent of the polygon feature is used.
  - If no clip geometry is provided, the entire result is copied to the output.
  - The projection is used to project the output results. The default is WGS84.
  - The outputs can be saved to the following formats: **File Geodatabase, Shapefile, Layer Package, or Map Package**.
  - If the output format is a File Geodatabase or Shapefile, the results are compressed into a zip file that can be downloaded. 
  - The clip results can optionally be added to a map document.
  - Instead of using a clip geometry, a clip feature class can be used to clip the input results.
  - An expression can be applied to the clip feature class to select a subset of of clip polygons.
  - If the output format is a layer package (LPK) or map package (MPK), the package file can be downloaded and opened directly in ArcMap.
  - The output data for layer and map packages is a file geodatabase.
  - If a search result is a layer file, it is copied, clipped, and re-sourced so symbology is maintained.
  - If a search result is a map document, the map document is copied and all it's layers are clipped and re-sourced.
  - Non-spatial files such as text files, PDF, and Office documents will be copied and included in the zip file or package.
  

###Screen Captures
![Clip Data] (imgs/clip_data_0.png "Clip Data example")

###Requirements
    - ArcGIS 10.x


[Voyager Search]:http://voyagersearch.com/
[@VoyagerGIS]:https://twitter.com/voyagergis
[github]:https://github.com/voyagersearch/tasks

    

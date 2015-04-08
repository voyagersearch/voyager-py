#Replace Data Source

Replaces an old data source for selected layer files and map document layers with a new data source. This task can be used to change the workspace path, workspace type, and / or change the dataset name.

###Usage Tips
  - The input search results must be layer files or map documents.
  - By default, back ups are created. Backups are created in the source location with a .bak extension. **RECOMMENDED**
  - The data source will only be updated if the new data source path is a valid.
  - A data source can be the the full catalog path to the dataset or it can be a workspace path when updating only the workspace portion of data.
  - If the dataset names are identical when switching workspaces, only the workspace paths are necessary. For example, a shapefile called Highways.shp can be redirected to a file geodatabase workspace if the dataset name in the file geodatabase is also called Highways.
  - If dataset names are different, full paths to the data source is required. For example, c:\data\highways.shp can be redirected to c:\data\usa.gdb\us_highways. 
  
###Screen Captures
This example shows replacing a personal geodatabase feature class with a new feature class in SDE.

![Replace Data Source] (imgs/replace_data_source_0.png "Replace Data Source example")

###Requirements
    - ArcGIS 10.x

[Voyager Search]:http://voyagersearch.com/
[@VoyagerGIS]:https://twitter.com/voyagergis
[github]:https://github.com/voyagersearch/tasks


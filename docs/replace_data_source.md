#Replace Data Source

Replaces an old data source for selected layer files and map document layers with a new data source. Unlike Replace Workspace Path, this task is used to change the workspace path, workspace type, and / or change the dataset name.

###Usage Tips
  - The input search results must be layer files or map documents.
  - By default, back ups are created. Backups are created in the source location with a .bak extension. **RECOMMENDED**
  - The data source will only be updated if the new data source path is a valid.
  - Partial paths can be updated. For example, a workspace path containing *C:\Data*, can be updated and replaced with *D:\Data*.

###Screen Captures
This example shows replacing a data source which was one a feature class in a personal geodatabase with a new data source which exist in SDE.

![Replace Data Source] (imgs/replace_data_source_0.png "Replace Data Source example")

###Requirements
    - ArcGIS 10.x

[Voyager Search]:http://voyagersearch.com/
[@VoyagerGIS]:https://twitter.com/voyagergis
[github]:https://github.com/voyagersearch/tasks


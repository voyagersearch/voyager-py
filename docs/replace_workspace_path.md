#Replace Workspace Path

Replaces an old workspace path for selected layer files and map document layers with a new workspace path.

###Usage Tips
  - The input search results must be layer files or map documents.
  - By default, back ups are created. Backups are created in the source location with a .bak extension. **RECOMMENDED**
  - The workspace will only be updated if the new workspace path is a valid workspace.
  For example, personal geodatabases can be replaced with file geodatabases. However, a geodatabase cannot be replaced by a folder.
  - Partial paths can be updated. For example, a workspace path containing *C:\Data*, can be updated and replaced with *D:\Data*.

###Screen Captures
![Replace Workspace Path] (imgs/replace_workspace_path_0.png "Replace Workspace Path example")

###Requirements
    - ArcGIS 10.x

[Voyager Search]:http://voyagersearch.com/
[@VoyagerGIS]:https://twitter.com/voyagergis
[github]:https://github.com/voyagersearch/tasks


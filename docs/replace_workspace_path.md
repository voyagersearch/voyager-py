#Replace Workspace Path

Replaces an old workspace path for selected layer files and map document layers with a new workspace path. It can't be used if the workspace type or dataset name has changed. It is ideal for scenarios where drive letters change, switch to UNC paths, update SDE connection file information, and so on.

###Usage Tips
  - The input search results must be layer files or map documents.
  - By default, back ups are created. Backups are created in the source location with a .bak extension. **RECOMMENDED**
  - The workspace will only be updated if the new workspace path is a valid workspace.
  - Partial paths can be updated. For example, a workspace path containing *C:\Data*, can be updated and replaced with *D:\Data*.

###Screen Captures
![Replace Workspace Path] (imgs/replace_workspace_path_0.png "Replace Workspace Path example")

###Requirements
    - ArcGIS 10.x

[Voyager Search]:http://voyagersearch.com/
[@VoyagerGIS]:https://twitter.com/voyagergis
[github]:https://github.com/voyagersearch/tasks


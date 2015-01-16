#Copy Files

Copies files to a target folder.

###Usage Tips
  - If the target folder does not exist, it will be created.
  - The input results must be file types and cannot include ArcGIS geodatabase datasets such as feature classes. However, a file geodatabase (.gdb) can be copied.
  - When copying Shapefiles, all supporting files such as .shp, .dbf, .shx, etc., are copied.
  - When copying Smart Data Compression files, all supporting files such as .sdc, .sdi, etc., are copied.
  - By default, a file's directory structure is maintained when copied.
  - By default, this task is only accessable to Voyager users whom are logged in or have administrator permissions. However, administrators can change these permissions by editing the copy_files.info.json file. At the top of the copy_files.info.json file, edit the "security" field to add new groups.
  

###Screen Captures
![Copy Files] (imgs/copy_files_0.png "Copy Files example")

[Voyager Search]:http://voyagersearch.com/
[@VoyagerGIS]:https://twitter.com/voyagergis
[github]:https://github.com/voyagersearch/tasks

    

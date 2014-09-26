#Move Files

Moves files to a target folder.

###Usage Tips
  - This task is equivalent to a cut and paste operation.
  - If the target folder does not exist, it will be created.
  - The input results must be file types and cannot include ArcGIS geodatabase datasets such as feature classes. However, a file geodatabase (.gdb) can be copied.
  - When moving Shapefiles, all supporting files such as .shp, .dbf, .shx, etc., are copied.
  - When moving Smart Data Compression files, all supporting files such as .sdc, .sdi, etc., are copied.
  - By default, a file's directory structure is maintained when copied. To move all items to a root folder, check on the 'Flatten Results' option.
  - By default, this task is only accessable to Voyager users who have administrator permissions. However, administrators can change these permissions by editing the move_files.info.json file. At the top of the move_files.info.json file, edit the "security" field to add new groups. To allow authenticated users to run this task, add an entry, "_LOGGEDIN".
  

[Voyager Search]:http://voyagersearch.com/
[@VoyagerGIS]:https://twitter.com/voyagergis
[github]:https://github.com/voyagersearch/tasks

    

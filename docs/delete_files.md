#Delete Files

Permanently delete files.

###Usage Tips
  - This task will permanently delete files. Please use caution when using this task.
  - The input results must be file types and cannot include ArcGIS geodatabase datasets such as feature classes. However, a file geodatabase (.gdb) can be deleted.
  - When deleting Shapefiles, all supporting files such as .shp, .dbf, .shx, etc., are copied.
  - When deleting Smart Data Compression files, all supporting files such as .sdc, .sdi, etc., are copied.
  - By default, this task is only accessable to Voyager users who have administrator permissions. However, administrators can change these permissions by editing the delete_files.info.json file. At the top of the delete_files.info.json file, edit the "security" field to add new groups. To allow authenticated users to run this task, add an entry, "_LOGGEDIN".
  

[Voyager Search]:http://voyagersearch.com/
[@VoyagerGIS]:https://twitter.com/voyagergis
[github]:https://github.com/voyagersearch/tasks

    

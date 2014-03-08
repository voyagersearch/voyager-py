voyager-processing
=====

VoyagerUtilityTools.tbx    -- contains tools for data management such as MakeLayerFiles and AddWKT.

Scripts Folder -- Python scripts required for each utility tool.

voyager_tasks folder -- contains the processing framework scripts.

VoyagerTaskRunner.py -- Used to execute a GP task. --info option returns parameter information for all tasks. Passing in .json file will execute a task.
Usage: VoyagerTaskRunner.py --info 
       VoyagerTaskRunner.py task.json
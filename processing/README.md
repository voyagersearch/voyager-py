Voyager Tasks
=====

TODO: provide a nicer description of the task framework here


See more task documentation in [/docs](docs)


###voyager_tasks folder 
  - Contains the processing framework task scripts.
  
###info folder
  - Contains files defining the parameters for each task. The prefix of the info file name is the same as the task script name.

###Task Runner script
  - VoyagerTaskRunner.py is used to execute a voyager task. The --info option returns parameter information for all tasks. Passing in .json file will execute a task.

###samples folder 
#####Sample scripts
  - index_files.py -- This Python script demonstrates how to send a set of files to voyager server for indexing.
  - submit_job_fme.py -- This Python script demonstrates how to run (POST) a FME Server service. 
       

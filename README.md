# j4fun

Java project built by maven. Key for first task - task1.Task{1/2}, for the second - task2.Task

The first task was soled in two ways: first one - pure sql over Spark Daset, and final - DSL over Spark dataset.
To check corectness of provided code please build project. Then run jar via command spark-submit --class task1.Task{1/2} --verbose j4fun-1.0.jar 'pathToFileReadFrom.csv' 'pathToWriteResultOfExecution.csv' as soon as you will get executable jar.
The first input parameter to jar is location of file in filesystem which is expected as initial data we provide execution for.
The second input parameter to jar is location of file in filesystem which is expected as file we write result to.

The second task was solved using SQL over Spark Dataset. Here initial first initial parameter is the same as mentioned above
but the second one is path to directory where subtasks will be persisted. So, in case you provide '/s2t/' directory as second 
parameter, you will get: /s2t/median.csv, /s2t/classification.csv, /s2t/ranked.csv.

Also, keep in mind yo always can look through my sql file loacted in resources. It was my draft :)

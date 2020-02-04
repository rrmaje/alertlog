#### Read Me First
Below is only a solution description to a given coding assignment.

There is no assumption about distribution of data in the file e.g. distance in number of lines between STARTED and FINISHED for a given id.


File is processed using batches of lines that can be grouped in memory. Batches are executed by a pool of threads managed by JVM.

Lines in the batch without corresponding event state are assigned to buckets in database. Bucket number is derived from id of the event state - so in the end only one bucket can contain event states for a given event.

Lines with matching ids are combined and inserted as Alerts into database if threshold is exceeded.

After all batches are executed, a process is started to combine records from each bucket into alerts.

Program attempts to optimize usage of memory and I/O using below parameters
* batchSize - max number of lines as input to worker thread - memory intensive - use smaller batches to avoid OutOfMemory exceptions(defaults to 100_000)
* maxLength - max number of lines to process(no limit by default)
* threshold - above which Alert is created in database(defaults to 4ms)
* threadPoolSize - number of worker threads(default 2 cores)
* numberOfBuckets - number of buckets indexed from 0 to store unmatched lines from single batch(defaults to 10)


#### Reference Documentation
To run the program:

`mvn package && java -jar target/alert-log-0.0.1-SNAPSHOT.jar <path to input file>`

Tests will run automatically.

To access results in HQSQLDB, 

`java -jar <path to hsqldb>/lib/sqltool.jar --inlineRc=url=jdbc:hsqldb:file:eventlog0,user=sa`

Alerts are stored in `event` table.


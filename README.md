# Workshop on Java-Spark at Janelia

[Apache Spark](https://spark.apache.org/) has proven itself to be a helpful tool for running jobs on the 
compute cluster.  Its main benefit is that it takes care of housekeeping and communication between the driver (master)
and the worker (executor) nodes.  For that it makes use of its core data structure, a so called Resilient Distributed 
Dataset (RDD). Any RDD can be mapped into a new RDD using user-specified functions.  Furthermore, Spark offers reduce
and reduce-like functionality for cases in which the result type differs from the calculation type.  Evaluation of RDDss
is lazy and happens only when requested. By default, Spark does not store the result of an operation (map or reduce-like)
on an RDD in memory, unless explicitly specified by the user.  For the Saalfeld lab, Spark has become a tool that
simplifies big data computation on the cluster and we would like to share tools and experience as well as receive
input to avoid redundante work effort in a tutorial on *Thursday, 04/02, 9:30am*.  In general, many labs would profit 
from the ease of use of Spark for large-scale projects.

If you want to chat about Spark at Janelia or ever have questions, we maintain a [chatroom](https://gitter.im/freeman-lab/spark-janelia) and you can usually find at least some of us in there!

**Note:** The chat is currently offline but might be back as a private chat in the future.

## Prerequisites

For the tutorial, please bring and prepare the following:
 - Laptop with
    - access to the intranet (vpn or ssh)
    - [Eclipse Luna](https://www.eclipse.org/downloads/packages/eclipse-ide-java-developers/lunasr2) with Maven integration
    - Posix shell and ssh-client
    - Git for accessing and modifying this repository
    - Maven installed in addition to Eclipse's m2e plugin
 - Build a project (ideally this workshop project) to fill your local Maven repository with all/most of the dependenceies

***
**NOTE**: Thanks to [Eric Perlman](https://github.com/perlman), we know that Eclipse converts symlinks to text
files when cloning a git repository. For that reason, clone this repository manually and then import the repository
as a Maven project from within Eclipse.
***

Spark jobs can run locally, either by defining the variable `spark.master=local` for the JVM, or by setting the
master to `"local"` in the Spark configuration within your Java program.  For the former, add `-Dspark.master=local`
to the run configuration for your class or your java call.  For the latter, call
```Java
SparkConf conf = new SparkConf()
     // set-up your config
     .setMaster("local")
;
```
In either case, `local` can be replaced by `local[<nThreads>]` or `local[*]` to specify the number of threads or use as
many threads as there are cpus on the local machine.

The best way of preparing Spark cluster jobs is to use Eclipse (or any  other appropriate Java IDE) to compile your
Java code in a location that is accessible from a cluster node.  That way, there is no need to transfer the jar files
onto the cluster and you can make sure you are alywas using the latest build of your software.  Before you start your
cluster job, run the project as Maven install from within eclipse to make sure the jar is up to date.  Make sure you
create an "uber" jar containing all dependencies as in our
[example pom.xml](https://github.com/saalfeldlab/java-spark-workshop/blob/master/pom.xml#L49-87). For any dependency
that is provided by the cluster, add `<scope>provided</scope>` to avoid reference clashes, e.g. for Spark:
```XML
  	<dependency>
  		<groupId>org.apache.spark</groupId>
  		<artifactId>spark-core_2.10</artifactId>
  		<version>1.2.1</version>
  		<scope>provided</scope>
  	</dependency>
```
This may lead to failure when running your Spark job locally through your IDE (it does so for IntellJ IDEA). Thus, for
local runs, change the scope of these dependencies to `compile`.

Once the uber jar is compiled and packaged, login to login1 and submit your spark job like this:
```bash
ssh login1
/path/to/java-spark-workshop/flintstone/flintstone.sh <MASTER_JOB_ID|N_NODES> /path/to/jar <class.to.be.Used> <ARGV>
qstat
```
`qstat` will give you the `<job-id>` for looking up the log file and the `<spark-master>`.  You can check the
status of your Spark job through the webinterface at `<spark-master>:8080`. Any standard output from the master
will be written to `~/.sparklogs/qsub-argument-wrap-script.sh.o<job-id>`.

### How to run an example
To run an example job (here: Sum of integers from 1 to N), after compiling (using Maven install inside Eclipse) the
project, run the appropriate `.sh` script in the project's root directory:
```bash
ssh login1
/path/to/java-spark-workshop/example-sum-of-integers.sh
qstat
sed -r 's/^M/\n/g' ~/.sparklogs/org.janelia.workshop.spark.IntegerSum.o<job-id> | less
```
The last line converts `^M` to newlines using `sed` and pipes the output to `less` for viewing. The expected result
of the computation is `N*(N+1)/2`. Change the value of `N` inside `example-sum-of-integers.sh` and compare with the
expected result.

For a detailed usage message for flintstone, run
```bash
/path/to/java-spark-workshop/flintstone/flintstone.sh
```
or for detailed info on Spark master and worker management, run
```bash
/usr/local/python-2.7.6/bin/python /usr/local/spark-versions/bin/spark-deploy.py -h
```

## Example Projects
 - *org.janelia.workshop.spark.IntegerSum*
   - Parameters (positional)
     - **N** - calculate sum from 1 to **N**
 - *org.janelia.workshop.spark.InjectionRendering*
   - Parameters
     - **-f/--format `<path>`** Format string for input files
     - **-o/--output `<path>`** Format string for output files
     - **-m/--minimum-index `<min>`** Use images starting with <min>
     - **-M/--maximun-index `<max>`** Use images until <max> (exclusive)
     - **-F/--from-file** Interpret *-f/--format* as path to a file that contains the names of input files
     - **-c/--coordinate-transform `<path>`** Path to file containing coordinate transform
     - **-n/--n-scans-per-section `<n>`** How many scans were performed for each section?
     - **-k/--k-nearest-neighbors` <k>`** Number of nearest neighbors for injection
 - *org.janelia.workshop.spark.SparkSimilarities*
   - Parameters
     - **-f/--format `<path>`** Format string for input files
     - **-o/--output `<path>`** Format string for output files
     - **-m/--minimum-index `<min>`** Use images starting with <min>
     - **-M/--maximun-index `<max>`** Use images until <max> (exclusive)
     - **-F/--from-file** Interpret *-f/--format* as path to a file that contains the names of input files
     - **-r/--range `<range>`** Maximum possible distance between images (in pixels) for calculation of similarity
     - **-s/--scale `<scale>`** Scale down by <scale> levels (powers of two)
 - *org.janelia.workshop.spark.Downsampling*
   - Parameters (positional)
     - **input format** Format String for input images
     - **min** Use images starting with *min*
     - **max** Use images until *max* (exclusive)
     - **level** Scale down by *level* levels (powers of two
     - **output format** Format String for output files

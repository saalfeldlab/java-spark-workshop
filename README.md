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

The best way of preparing Spark jobs is to use Eclipse to compile your Java code in a location that is accessible
from a cluster node.  That way, there is no need to transfer the jar files onto the cluster and you can make sure
you are alywas using the latest build of your software.  Before you start your cluster job, run the project as
Maven install from within eclipse to make sure the jar is up to date. Once that is done, login to login1 and submit
your spark job like this:
```bash
ssh login1
/path/to/java-spark-workshop/inflame.sh <N_NODES> /path/to/jar <class.to.be.Used> <ARGV>
qstat
```
`qstat` will give you the `<job-id>` for looking up the log file and the `<spark-master>`.  You can check the
status of your Spark job through the webinterface at `<spark-master>:8080`. Any standard output from the master
will be written to `~/.sparklogs/qsub-argument-wrap-script.sh.o<job-id>`.

### How to run an example
To run the example job (Sum of integers from 1 to N), after compiling (using Maven install inside Eclipse) the
project, do:
```bash
ssh login1
/path/to/java-spark-workshop/example-sum-of-integers.sh
qstat
sed -r 's/^M/\n/g' ~/.sparklogs/qsub-argument-wrap-script.sh.o<job-id> | less
```
The last line converts `^M` to newlines and pipes the output to less for viewing. The expected result of the
computation is `N*(N+1)/2`. Change the value of `N` inside `example-sum-of-integers.sh` and compare with the
result.

## Example Projects
 - *org.janelia.workshop.spark.IntegerSum*
   - Parameters
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

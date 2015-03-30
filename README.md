# Workshop on Java-Spark at Janelia

[Apache Spark](https://spark.apache.org/) has proven itself to be a helpful tool for running jobs on the 
compute cluster.  Its main benefit is that it takes care of housekeeping and communication between the driver (master)
and the worker (executor) nodes.  For that it makes use of its core data structure, a so called Resilient Distributed 
Dataset (RDD). Any RDD can be mapped into a new RDD using user-specified functions.  Furthermore, Spark offers reduce
and reduce-like functionality for cases in which the result type differs from the calculation type.  Evaluation of RDDss
is lazy and happens only when requested. By default, Spark does not store the result of an operation (map or reduce-like)
on an RDD in memory, unless explicitly specified by the user.  For the Saalfeld lab, Spark has become a tool that
simplifies big data computation on the cluster and we would like to share tools and experience as well as receive
input to avoid redundante work effort in a tutorial on Thursday, 04/02, 9:30am.  In general, many labs would profit 
from the ease of use of Spark for large-scale projects.

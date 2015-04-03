# flintstone
Ingition for your spark jobs

The easiest way is to just call
```bash
<flintstone-root>/src/main/shell/inflame.sh <N_NODES> <JAR> <CLASS> <ARGV>
```
where
 - `<N_NODES` - number of worker nodes
 - `<JAR>` - path to jar containing your class and all dependencies
 - `<CLASS>` - class that holds spark main function
 - `<ARGV>` - any arguments passed to your class

This will create a spark launcher from `<flintstone-root>/src/main/shell/template.sh` for you and run it using the appropriate arguments. In most cases, it makes sense to wrap the call to `inflame.sh` into a script that also stores the complete `<ARGV>` for reproducability and ease of use in case of a very long `<ARGV>`.


For more flexibility, create your spark launcher `/path/to/your/script.sh` for your Java Spark main class like:
https://github.com/saalfeldlab/flintstone/blob/master/src/main/shell/get-sum-of-integers.sh

You can run 
```bash
<flintstone-root>/src/main/shell/ignite.sh <N> <SCRIPT> <ARGV>
```
where 
 - `<N>` is the number of nodes
 - `<SCRIPT>` is the `/path/to/your/script.sh`
 - `<ARGV>` is the argument vector for your script

For an example, build the project, then ssh to `login1` and run:
```bash
cd <flintstone-root>/example
./example.sh
```
`example.sh` will call `<flintstone-root>/example/get-sum-of-integers.sh` as a script.
Equivalently, you could also call:
```bash
<flintstone-root>/src/main/shell/inflame.sh <N_NODES> <flintstone-root>/target/flintstone-0.0.1-SNAPSHOT.jar org.janelia.flintstone.Example <N>
```

For convenience, you can copy the contents of `<flintstone-root>/src/main/shell` to `/any/other/location`. Make sure that all four scripts and `template.sh` are present at `/any/other/location`.


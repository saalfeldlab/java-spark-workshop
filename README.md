# flintstone
Ingition for your spark jobs

Create your spark launcher `/path/to/your/script.sh` for your Java Spark main class like:
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

For convenience, you can copy the contents of `<flintstone-root>/src/main/shell` to `/any/other/location`. Make sure that all three scripts are present at `/any/other/location`.


# flintstone
Ingition for your spark jobs

Create your spark launcher `/path/to/your/script.sh` for your Java Spark main class like:
https://github.com/saalfeldlab/flintstone/blob/master/src/main/shell/get-sum-of-integers.sh

Copy
https://github.com/saalfeldlab/flintstone/blob/master/src/main/shell/example.sh
to `/path/to/copy/of/example.sh` and modify `$ROOT_DIR` such that it points to `/path/to/flintstone/src/main/shell` and `$SCRIPT` such that it points to `/path/to/your/script.sh`. Modify `$ARGV` to fit the requirements of your Java Spark main class.

`ssh login1` and execute
```bash
N_NODES=<N> /path/to/copy/of/example.sh
```

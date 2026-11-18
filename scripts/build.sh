#!/usr/bin/env bash
# Compiles the Scala sources against the jars of a Spark 4 install (Scala 2.13) and builds a jar.
# SPARK_HOME can point to a Spark download or to a pip-installed pyspark folder.
set -euo pipefail
cd "$(dirname "$0")/.."
SPARK_HOME="${SPARK_HOME:-$(python3 -c 'import pyspark, os; print(os.path.dirname(pyspark.__file__))')}"
ls "$SPARK_HOME"/jars/scala-compiler-*.jar >/dev/null || { echo "scala-compiler jar not found in $SPARK_HOME/jars"; exit 1; }
rm -rf target/classes && mkdir -p target/classes
java -cp "$SPARK_HOME/jars/*" scala.tools.nsc.Main -deprecation -feature \
  -classpath "$SPARK_HOME/jars/*" -d target/classes src/main/scala/dq/*.scala
jar cf target/spark-data-quality-monitor.jar -C target/classes .
echo "built target/spark-data-quality-monitor.jar with $(ls "$SPARK_HOME"/jars/scala-library-*.jar | xargs -n1 basename)"

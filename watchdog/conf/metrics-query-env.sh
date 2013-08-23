# Set environment variables here.

# Main class to be invoked.
MAIN_CLASS=com.continuuity.metrics.runtime.MetricsQueryMain

# Arguments for main class.
MAIN_CLASS_ARGS=

# Add Hadoop HDFS classpath
EXTRA_CLASSPATH="$HBASE_HOME/conf/"

JAVA_HEAPMAX=-Xmx1024m
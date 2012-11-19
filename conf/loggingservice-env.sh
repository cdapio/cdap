# Main class to be invoked.
MAIN_CLASS=com.continuuity.common.logging.FlumeLogCollector

# Arguments for main class.
#MAIN_CLASS_ARGS=""

# Add Hadoop HDFS classpath
EXTRA_CLASSPATH="$CONTINUUITY_HOME/datafabric/conf:$YARN_HOME/conf/"

# Specify Heap Size.
JAVA_HEAPMAX=-Xmx1024m

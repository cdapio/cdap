# Set environment variables here.

# Main class to be invoked.
MAIN_CLASS=com.continuuity.data.runtime.main.ReactorServiceMain

# Arguments for main class.
MAIN_CLASS_ARGS="start"

# Add Hadoop HDFS classpath
# Assuming update-alternatives convention
EXTRA_CLASSPATH="/etc/hbase/conf/"

JAVA_HEAPMAX=-Xmx1024m


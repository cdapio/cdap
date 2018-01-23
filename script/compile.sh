HOSTNAME=<INSERT_HOSTNAME_HERE>

javac -cp '/Users/alianwar/Downloads/cdap-sandbox-5.0.0-SNAPSHOT/lib/*:/Users/alianwar/.m2/repository/org/apache/hbase/hbase-common/1.0.1.1/hbase-common-1.0.1.1.jar' /Users/alianwar/dev/cdap/cdap-explore/src/main/java/co/cask/cdap/explore/executor/HiveJdbcClient.java \
&& scp /Users/alianwar/dev/cdap/cdap-explore/src/main/java/co/cask/cdap/explore/executor/HiveJdbcClient.class $HOSTNAME: \
&& rm /Users/alianwar/dev/cdap/cdap-explore/src/main/java/co/cask/cdap/explore/executor/HiveJdbcClient.class \
&& scp /Users/alianwar/dev/cdap/cdap-explore/src/main/java/co/cask/cdap/explore/executor/HiveJdbcClient\$1.class $HOSTNAME: \
&& rm /Users/alianwar/dev/cdap/cdap-explore/src/main/java/co/cask/cdap/explore/executor/HiveJdbcClient\$1.class \
&& scp /Users/alianwar/dev/cdap/cdap-explore/src/main/java/co/cask/cdap/explore/executor/HiveJdbcClient\$2.class $HOSTNAME: \
&& rm /Users/alianwar/dev/cdap/cdap-explore/src/main/java/co/cask/cdap/explore/executor/HiveJdbcClient\$2.class

package com.continuuity.explore.guice;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.List;

/**
 * Test LocalMapreduceClasspathSetter.
 */
public class LocalMapreduceClasspathSetterTest {
  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();
  
  @Test
  public void testClasspathSetupMulti() throws Exception {
    System.clearProperty(HiveConf.ConfVars.HADOOPBIN.toString());

    List<String> inputURLs = Lists.newArrayList();
    inputURLs.add("/usr/lib/hbase/lib/hbase-protocol-0.96.1.2.0.11.0-1-hadoop2.jar");
    inputURLs.add("/opt/cloudera/parcels/CDH-5.0.0-0.cdh5b2.p0.27/lib/hbase/hbase-protocol-0.95.0.jar");
    inputURLs.add("/home/cloudera/.m2/repository/org/apache/hbase/hbase-protocol/0.95.1-hadoop1/" +
                             "hbase-protocol-0.95.1-hadoop1.jar");
    inputURLs.add("/usr/lib/hbase/lib/hbase-hadoop2-compat-0.96.1.2.0.11.0-1-hadoop2.jar");
    inputURLs.add("/usr/lib/hbase/lib/hbase-thrift-0.96.1.2.0.11.0-1-hadoop2.jar");
    inputURLs.add("/usr/lib/hadoop/hadoop-common-2.2.0.2.0.11.0-1-tests.jar");

    HiveConf hiveConf = new HiveConf();
    LocalMapreduceClasspathSetter classpathSetter =
      new LocalMapreduceClasspathSetter(hiveConf, TEMP_FOLDER.newFolder().getAbsolutePath());

    for (String url : inputURLs) {
      classpathSetter.accept(url);
    }

    Assert.assertEquals(ImmutableList.of("/usr/lib/hbase/lib/hbase-protocol-0.96.1.2.0.11.0-1-hadoop2.jar",
                                         "/opt/cloudera/parcels/CDH-5.0.0-0.cdh5b2.p0.27/lib/hbase/" +
                                           "hbase-protocol-0.95.0.jar",
                                         "/home/cloudera/.m2/repository/org/apache/hbase/hbase-protocol/" +
                                           "0.95.1-hadoop1/hbase-protocol-0.95.1-hadoop1.jar"),
                        ImmutableList.copyOf(classpathSetter.getHbaseProtocolJarPaths()));

    classpathSetter.setupClasspathScript();
    
    String newHadoopBin = new HiveConf().get(HiveConf.ConfVars.HADOOPBIN.toString());
    Assert.assertEquals(generatedHadoopBinMulti,
                        Joiner.on('\n').join(Files.readLines(new File(newHadoopBin), Charsets.UTF_8)));
    Assert.assertTrue(new File(newHadoopBin).canExecute());
    System.clearProperty(HiveConf.ConfVars.HADOOPBIN.toString());
  }

  @Test
  public void testClasspathSetupSingle() throws Exception {
    System.clearProperty(HiveConf.ConfVars.HADOOPBIN.toString());

    List<String> inputURLs = Lists.newArrayList();
    inputURLs.add("/usr/lib/hbase/lib/hbase-hadoop2-compat-0.96.1.2.0.11.0-1-hadoop2.jar");
    inputURLs.add("/usr/lib/hbase/lib/hbase-thrift-0.96.1.2.0.11.0-1-hadoop2.jar");
    inputURLs.add("/usr/lib/hbase/lib/hbase-protocol-0.96.1.2.0.11.0-1-hadoop2.jar");
    inputURLs.add("/usr/lib/hadoop/hadoop-common-2.2.0.2.0.11.0-1-tests.jar");

    HiveConf hiveConf = new HiveConf();
    LocalMapreduceClasspathSetter classpathSetter =
      new LocalMapreduceClasspathSetter(hiveConf, TEMP_FOLDER.newFolder().getAbsolutePath());

    for (String url : inputURLs) {
      classpathSetter.accept(url);
    }

    Assert.assertEquals(ImmutableList.of("/usr/lib/hbase/lib/hbase-protocol-0.96.1.2.0.11.0-1-hadoop2.jar"),
                        ImmutableList.copyOf(classpathSetter.getHbaseProtocolJarPaths()));

    classpathSetter.setupClasspathScript();

    String newHadoopBin = new HiveConf().get(HiveConf.ConfVars.HADOOPBIN.toString());
    Assert.assertEquals(generatedHadoopBinSingle,
                        Joiner.on('\n').join(Files.readLines(new File(newHadoopBin), Charsets.UTF_8)));
    Assert.assertTrue(new File(newHadoopBin).canExecute());
    System.clearProperty(HiveConf.ConfVars.HADOOPBIN.toString());
  }

  @Test
  public void testClasspathSetupNone() throws Exception {
    System.clearProperty(HiveConf.ConfVars.HADOOPBIN.toString());
    String originalHadoopBin = new HiveConf().get(HiveConf.ConfVars.HADOOPBIN.toString());

    List<String> inputURLs = Lists.newArrayList();
    inputURLs.add("/usr/lib/hbase/lib/hbase-hadoop2-compat-0.96.1.2.0.11.0-1-hadoop2.jar");
    inputURLs.add("/usr/lib/hbase/lib/hbase-thrift-0.96.1.2.0.11.0-1-hadoop2.jar");
    inputURLs.add("/usr/lib/hadoop/hadoop-common-2.2.0.2.0.11.0-1-tests.jar");

    HiveConf hiveConf = new HiveConf();
    LocalMapreduceClasspathSetter classpathSetter =
      new LocalMapreduceClasspathSetter(hiveConf, TEMP_FOLDER.newFolder().getAbsolutePath());

    for (String url : inputURLs) {
      classpathSetter.accept(url);
    }

    Assert.assertTrue(classpathSetter.getHbaseProtocolJarPaths().isEmpty());

    classpathSetter.setupClasspathScript();

    String newHadoopBin = new HiveConf().get(HiveConf.ConfVars.HADOOPBIN.toString());
    Assert.assertEquals(originalHadoopBin, newHadoopBin);
    System.clearProperty(HiveConf.ConfVars.HADOOPBIN.toString());
  }

  private static final String generatedHadoopBinMulti =
    "#!/usr/bin/env bash\n" +
      "# This file is a hack to set HADOOP_CLASSPATH for Hive local mapreduce tasks.\n" +
      "# This hack should go away when Twill supports setting of environmental variables for a TwillRunnable" +
      " - REACTOR-325.\n" +
      "\n" +
      "function join { local IFS=\"$1\"; shift; echo \"$*\"; }\n" +
      "if [ $# -ge 1 -a \"$1\" = \"jar\" ]; then\n" +
      "  HADOOP_CLASSPATH=$(join : ${HADOOP_CLASSPATH} " +
      "/usr/lib/hbase/lib/hbase-protocol-0.96.1.2.0.11.0-1-hadoop2.jar " +
      "/opt/cloudera/parcels/CDH-5.0.0-0.cdh5b2.p0.27/lib/hbase/hbase-protocol-0.95.0.jar " +
      "/home/cloudera/.m2/repository/org/apache/hbase/hbase-protocol/0.95.1-hadoop1/" +
      "hbase-protocol-0.95.1-hadoop1.jar)\n" +
      "  export HADOOP_CLASSPATH\n" +
      "  echo \"Explore modified HADOOP_CLASSPATH = $HADOOP_CLASSPATH\" 1>&2\n" +
      "fi\n" +
      "\n" +
      "exec /usr/bin/hadoop \"$@\"";

  private static final String generatedHadoopBinSingle =
    "#!/usr/bin/env bash\n" +
      "# This file is a hack to set HADOOP_CLASSPATH for Hive local mapreduce tasks.\n" +
      "# This hack should go away when Twill supports setting of environmental variables for a TwillRunnable" +
      " - REACTOR-325.\n" +
      "\n" +
      "function join { local IFS=\"$1\"; shift; echo \"$*\"; }\n" +
      "if [ $# -ge 1 -a \"$1\" = \"jar\" ]; then\n" +
      "  HADOOP_CLASSPATH=$(join : ${HADOOP_CLASSPATH} /usr/lib/hbase/lib/" +
      "hbase-protocol-0.96.1.2.0.11.0-1-hadoop2.jar)\n" +
      "  export HADOOP_CLASSPATH\n" +
      "  echo \"Explore modified HADOOP_CLASSPATH = $HADOOP_CLASSPATH\" 1>&2\n" +
      "fi\n" +
      "\n" +
      "exec /usr/bin/hadoop \"$@\"";
}

/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.explore.guice;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.io.Files;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * This class hijacks Hadoop bin, and adds hbase-protocol jar to HADOOP_CLASSPATH.
 * This hack should go away when Twill supports setting of environmental variables for a TwillRunnable.
 * Addition info on why this is needed is in CDAP-9
 */
public class LocalMapreduceClasspathSetter {
  private static final Logger LOG = LoggerFactory.getLogger(LocalMapreduceClasspathSetter.class);
  private static final Pattern HBASE_PROTOCOL_JAR_NAME_PATTERN =
    Pattern.compile(".*" + File.separatorChar + "hbase-protocol-.+\\.jar$");

  private final HiveConf hiveConf;
  private final String directory;
  private final List<String> hiveAuxJars;
  private final Set<String> hbaseProtocolJarPaths = new LinkedHashSet<String>();

  public LocalMapreduceClasspathSetter(HiveConf hiveConf, String directory, List<String> hiveAuxJars) {
    this.hiveConf = hiveConf;
    this.directory = directory;
    this.hiveAuxJars = hiveAuxJars;
  }

  public void accept(String jar) {
    if (HBASE_PROTOCOL_JAR_NAME_PATTERN.matcher(jar).matches()) {
      hbaseProtocolJarPaths.add(jar);
    }
  }

  public void setupClasspathScript() throws IOException {
    if (hbaseProtocolJarPaths.isEmpty()) {
      LOG.info("No HBase Protocol jar found. Not setting up HADOOP_CLASSPATH");
      return;
    }

    File exploreHadoopBin = new File(directory, "explore_hadoop");

    LOG.info("Adding {} to HADOOP_CLASSPATH", hbaseProtocolJarPaths);

    String hadoopBin = hiveConf.get(HiveConf.ConfVars.HADOOPBIN.toString());

    // We over-ride HADOOPBIN setting in HiveConf to the script below, so that Hive uses this script to execute
    // map reduce jobs.
    // The below script updates HADOOP_CLASSPATH to contain hbase-protocol jar for RunJar commands,
    // so that the right version of protocol buffer jar gets loaded for HBase.
    // It also puts all the user jars, ie hive aux jars, in this classpath and in first position, so that
    // the right version of ASM jar gets loaded for Twill.
    // It then calls the real Hadoop bin with the same arguments.
    StringBuilder fileBuilder = new StringBuilder();
    fileBuilder.append("#!/usr/bin/env bash\n");
    fileBuilder.append("# This file is a hack to set HADOOP_CLASSPATH for Hive local mapreduce tasks.\n");
    fileBuilder.append("# This hack should go away when Twill supports setting of environmental variables for a ");
    fileBuilder.append("TwillRunnable.\n");
    fileBuilder.append("\n");
    fileBuilder.append("function join { local IFS=\"$1\"; shift; echo \"$*\"; }\n");
    fileBuilder.append("if [ $# -ge 1 -a \"$1\" = \"jar\" ]; then\n");
    fileBuilder.append("  HADOOP_CLASSPATH=$(join ").append(File.pathSeparatorChar)
      .append(" ").append(Joiner.on(' ').join(hiveAuxJars))
      .append(" ${HADOOP_CLASSPATH} ")
      .append(Joiner.on(' ').join(hbaseProtocolJarPaths)).append(')').append("\n");
    fileBuilder.append("  # Put user jars first in Hadoop classpath so that the ASM jar needed by Twill has\n");
    fileBuilder.append("  # the right version, and not the one provided with the Hadoop libs.\n");
    fileBuilder.append("  export HADOOP_USER_CLASSPATH_FIRST=true\n");
    fileBuilder.append("  export HADOOP_CLASSPATH\n");
    fileBuilder.append("  echo \"Explore modified HADOOP_CLASSPATH = $HADOOP_CLASSPATH\" 1>&2\n");
    fileBuilder.append("fi\n");
    fileBuilder.append("\n");
    fileBuilder.append("exec ").append(hadoopBin).append(" \"$@\"\n");

    Files.write(fileBuilder.toString(), exploreHadoopBin, Charsets.UTF_8);

    if (!exploreHadoopBin.setExecutable(true, false)) {
      throw new RuntimeException("Cannot set executable permission on " + exploreHadoopBin.getAbsolutePath());
    }

    LOG.info("Setting Hadoop bin to Explore Hadoop bin {}", exploreHadoopBin.getAbsolutePath());
    System.setProperty(HiveConf.ConfVars.HADOOPBIN.toString(), exploreHadoopBin.getAbsolutePath());
  }

  Set<String> getHbaseProtocolJarPaths() {
    return hbaseProtocolJarPaths;
  }
}

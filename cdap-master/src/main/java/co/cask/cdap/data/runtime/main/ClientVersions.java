/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.cdap.data.runtime.main;

import co.cask.cdap.common.utils.ProjectInfo;
import co.cask.cdap.data2.util.hbase.HBaseVersion;
import org.apache.hadoop.util.VersionInfo;
import org.apache.kafka.clients.KafkaClient;
import org.apache.zookeeper.version.Info;

import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Provides client versions of infrastructure components.
 */
public class ClientVersions {

  public static String getCdapVersion() {
    return ProjectInfo.getVersion().toString();
  }

  public static String getCdapHBaseCompatVersion() {
    // cdap hbase compat module
    return HBaseVersion.get().toString();
  }

  public static String getTephraHBaseCompatVersion() {
    // tephra hbase compat module
    return org.apache.tephra.util.HBaseVersion.get().toString();
  }


  public static String getHadoopVersion() {
    return VersionInfo.getVersion();
  }

  public static String getHBaseVersion() {
    return HBaseVersion.getVersionString();
  }

  public static String getZooKeeperVersion() {
    return String.format("%d.%d.%d.%d", Info.MAJOR, Info.MINOR, Info.MICRO, Info.REVISION);
  }

  public static String getKafkaVersion() {
    URL kafkaJar = KafkaClient.class.getResource("/" + KafkaClient.class.getName().replace(".", "/") + ".class");
    // kafkaJar.getPath() looks like jar:file:/a/b/c/d/kafka-clients-0.8.2.2.jar
    String[] tokens = kafkaJar.getPath().split("!")[0].split("/");
    String jarFilename = tokens[tokens.length - 1];
    Matcher matcher = Pattern.compile("kafka-clients-(\\d+\\.\\d+\\.\\d+\\.\\d+)\\.jar").matcher(jarFilename);
    if (matcher.find()) {
      return matcher.group(1);
    }

    return "unknown";
  }

  public static void main(String[] args) {
    System.out.println("Hadoop version: " + ClientVersions.getHadoopVersion());
    System.out.println("HBase version: " + ClientVersions.getHBaseVersion());
    System.out.println("ZooKeeper version: " + ClientVersions.getZooKeeperVersion());
    System.out.println("Kafka version: " + ClientVersions.getKafkaVersion());

    System.out.println("CDAP version: " + ClientVersions.getCdapVersion());
    System.out.println("CDAP HBase compat version: " + ClientVersions.getCdapHBaseCompatVersion());
    System.out.println("Tephra HBase compat version: " + ClientVersions.getTephraHBaseCompatVersion());
  }

}

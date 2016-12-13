/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.operations.hbase;

import co.cask.cdap.data2.util.hbase.HBaseVersion;
import co.cask.cdap.operations.OperationalStats;
import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

/**
 * {@link OperationalStats} representing HBase information.
 */
public class HBaseInfo extends AbstractHBaseStats implements HBaseInfoMXBean {

  private String webUrl;
  private String logsUrl;

  @SuppressWarnings("unused")
  public HBaseInfo() {
    this(HBaseConfiguration.create());
  }

  @VisibleForTesting
  HBaseInfo(Configuration conf) {
    super(conf);
  }

  @Override
  public String getStatType() {
    return "info";
  }

  @Override
  public String getVersion() {
    return HBaseVersion.getVersionString();
  }

  @Override
  public String getWebURL() {
    return webUrl;
  }

  @Override
  public String getLogsURL() {
    return logsUrl;
  }

  @Override
  public synchronized void collect() throws IOException {
    try (HBaseAdmin admin = new HBaseAdmin(conf)) {
      ClusterStatus clusterStatus = admin.getClusterStatus();
      ServerName master = clusterStatus.getMaster();
      String protocol = conf.getBoolean("hbase.ssl.enabled", false) &&
        conf.get("hbase.http.policy").equals("HTTPS_ONLY") ?
        "https" : "http";
      webUrl = String.format("%s://%s:%s", protocol, master.getHostname(), conf.get("hbase.master.info.port"));
      logsUrl = webUrl + "/logs";
    }
  }
}

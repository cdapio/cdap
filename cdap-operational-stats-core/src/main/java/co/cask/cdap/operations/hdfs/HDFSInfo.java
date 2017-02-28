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

package co.cask.cdap.operations.hdfs;

import co.cask.cdap.operations.OperationalStats;
import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.VersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * {@link OperationalStats} representing HDFS information.
 */
public class HDFSInfo extends AbstractHDFSStats implements HDFSInfoMXBean {
  private static final Logger LOG = LoggerFactory.getLogger(HDFSInfo.class);

  @VisibleForTesting
  static final String STAT_TYPE = "info";

  @SuppressWarnings("unused")
  public HDFSInfo() {
    this(new Configuration());
  }

  @VisibleForTesting
  HDFSInfo(Configuration conf) {
    super(conf);
  }

  @Override
  public String getStatType() {
    return STAT_TYPE;
  }

  @Override
  public String getVersion() {
    return VersionInfo.getVersion();
  }

  @Override
  public String getLogsURL() {
    String webURL = getWebURL();
    if (webURL == null) {
      return null;
    }
    return webURL + "/logs";
  }

  @Override
  public void collect() throws IOException {
    // No need to refresh static information
  }
}

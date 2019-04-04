/*
 * Copyright © 2018 Cask Data, Inc.
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

package io.cdap.cdap.gateway.handlers.util;

import com.google.common.io.Resources;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.internal.app.spark.SparkCompatReader;
import io.cdap.cdap.proto.ClientVersion;
import org.apache.hadoop.util.VersionInfo;
import org.apache.zookeeper.version.Info;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Util class to determine version of clients CDAP is using
 */
public class VersionHelper {
  private static final Logger LOG = LoggerFactory.getLogger(VersionHelper.class);

  private VersionHelper() {
  }

  public static ClientVersion getCDAPVersion() {
    try {
      String version = Resources.toString(Resources.getResource("VERSION"), StandardCharsets.UTF_8);
      if (!version.equals("${project.version}")) {
        return new ClientVersion("cdap", version.trim());
      }
    } catch (IOException e) {
      LOG.warn("Failed to determine current version", e);
    }
    return new ClientVersion("cdap", "unknown");
  }

  public static ClientVersion getHadoopVersion() {
    return new ClientVersion("hadoop", VersionInfo.getVersion());
  }

  public static ClientVersion getZooKeeperVersion() {
    return new ClientVersion("zookeeper",
                             String.format("%d.%d.%d.%d", Info.MAJOR, Info.MINOR, Info.MICRO, Info.REVISION));
  }

  public static ClientVersion getSparkVersion(CConfiguration cConf) {
    return new ClientVersion("sparkcompat", SparkCompatReader.get(cConf).getCompat());
  }
}

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

package co.cask.cdap.internal.app.runtime.batch.inmemory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.LocalJobRunnerWithFix;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.protocol.ClientProtocol;
import org.apache.hadoop.mapreduce.protocol.ClientProtocolProvider;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * This is the copy of {@link org.apache.hadoop.mapred.LocalClientProtocolProvider} which provides the version of
 * LocalJobRunner with fix.
 */
public class LocalClientProtocolProvider extends ClientProtocolProvider {
  private static final Logger LOG = LoggerFactory.getLogger(LocalClientProtocolProvider.class);

  @Override
  public ClientProtocol create(Configuration conf) throws IOException {
    String framework =
      conf.get(MRConfig.FRAMEWORK_NAME, MRConfig.LOCAL_FRAMEWORK_NAME);
    LOG.debug("Using framework: " + framework);
    if (!MRConfig.LOCAL_FRAMEWORK_NAME.equals(framework)) {
      return null;
    }

    // We have to use something unique like "clocal" to make sure Hadoop's LocalClientProtocolProvider will fail to
    // provide the ClientProtocol
    String tracker = conf.get(JTConfig.JT_IPC_ADDRESS, "clocal");
    LOG.info("Using tracker: " + tracker);

    if ("clocal".equals(tracker)) {
      conf.setInt("mapreduce.job.maps", 1);
      return new LocalJobRunnerWithFix(conf);
    } else {

      throw new IOException("Invalid \"" + JTConfig.JT_IPC_ADDRESS
                              + "\" configuration value for LocalJobRunner : \""
                              + tracker + "\"");
    }
  }

  @Override
  public ClientProtocol create(InetSocketAddress addr, Configuration conf) {
    return null; // LocalJobRunner doesn't use a socket
  }

  @Override
  public void close(ClientProtocol clientProtocol) {
    // no clean up required
  }
}

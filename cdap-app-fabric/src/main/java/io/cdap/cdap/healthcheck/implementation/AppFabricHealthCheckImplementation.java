/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.healthcheck.implementation;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.inject.Inject;
import com.sun.management.HotSpotDiagnosticMXBean;
import com.sun.tools.hat.internal.model.JavaHeapObject;
import com.sun.tools.hat.internal.parser.HprofReader;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.DirUtils;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.net.MalformedURLException;
import java.util.Enumeration;
import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import static io.cdap.cdap.data2.datafabric.dataset.DatasetServiceClient.append;

public class AppFabricHealthCheckImplementation {
  private static final Logger LOG = LoggerFactory.getLogger(AppFabricHealthCheckImplementation.class);

  private static final Gson GSON = new GsonBuilder().create();
  private static final long MAX_PORT = (1 << 16) - 1;
  private static final String SERVICE_URL_FORMAT = "service:jmx:rmi:///jndi/rmi://%s:%s/jmxrmi";
  private final CConfiguration cConf;
  private final JMXServiceURL serviceUrl;

  @Inject
  public AppFabricHealthCheckImplementation(CConfiguration cConf) throws MalformedURLException {
    this.cConf = cConf;
    int serverPort = cConf.getInt(Constants.JMXMetricsCollector.SERVER_PORT);
    if (serverPort < 0 || serverPort > MAX_PORT) {
      throw new IllegalArgumentException(
        String.format("%s variable (%d) is not a valid port number.", Constants.JMXMetricsCollector.SERVER_PORT,
                      serverPort));
    }
    String serverUrl = String.format(SERVICE_URL_FORMAT, "localhost", serverPort);

    this.serviceUrl = new JMXServiceURL(serverUrl);
  }

  public HealthCheckResponse collect() {
    JsonArray podInfoList = GSON.fromJson(cConf.get(Constants.AppFabricHealthCheck.POD_INFO), JsonArray.class);
    JsonArray nodeInfoList = GSON.fromJson(cConf.get(Constants.AppFabricHealthCheck.NODE_INFO), JsonArray.class);
    JsonArray eventInfoList = GSON.fromJson(cConf.get(Constants.AppFabricHealthCheck.EVENT_INFO), JsonArray.class);
    String threadDump = "";
    String heapDump = "";
    try (JMXConnector jmxConnector = JMXConnectorFactory.connect(serviceUrl, null)) {
      MBeanServerConnection mBeanConn = jmxConnector.getMBeanServerConnection();
      heapDump = getHeapDump(cConf, mBeanConn);
      threadDump = getThreadDump(mBeanConn);
    } catch (IOException e) {
      LOG.error("Can not obtain client", e);
    }
    return HealthCheckResponse.named("Health check with data")
      .withData("podList", GSON.toJson(podInfoList))
      .withData("nodeList", GSON.toJson(nodeInfoList))
      .withData("eventList", GSON.toJson(eventInfoList))
      .withData("heapDump", heapDump)
      .withData("threadDump", threadDump)
      .up().build();
  }

  private static String getHeapDump(CConfiguration cConf, MBeanServerConnection mBeanConn) throws IOException {
    File tmpDir =
      new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR), cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
    DirUtils.mkdirs(tmpDir);
    HotSpotDiagnosticMXBean mxBean =
      ManagementFactory.newPlatformMXBeanProxy(mBeanConn, "com.sun.management:type=HotSpotDiagnostic",
                                               HotSpotDiagnosticMXBean.class);
    File heapDumpTempFile = File.createTempFile("appfabric-heap-dump", ".hprof", tmpDir);
    if (heapDumpTempFile.exists()) {
      heapDumpTempFile.delete();
    }
    mxBean.dumpHeap(heapDumpTempFile.getPath(), true);
    Enumeration<JavaHeapObject> snapshot = HprofReader.readFile(heapDumpTempFile.getPath(), true, 1).getThings();

    heapDumpTempFile.delete();

    return snapshot.toString();
  }

  private static String getThreadDump(MBeanServerConnection mBeanConn) throws IOException {
    ThreadMXBean threadMXBean =
      ManagementFactory.newPlatformMXBeanProxy(mBeanConn, ManagementFactory.THREAD_MXBEAN_NAME, ThreadMXBean.class);
    StringBuilder threadDump = new StringBuilder(System.lineSeparator());
    ThreadInfo[] threadInfos = threadMXBean.dumpAllThreads(true, true);
    for (ThreadInfo threadInfo : threadInfos) {
      append(threadDump, threadInfo);
    }
    return threadDump.toString();
  }
}

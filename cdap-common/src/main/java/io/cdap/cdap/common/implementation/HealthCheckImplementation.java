/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.common.implementation;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.net.MalformedURLException;

public class HealthCheckImplementation {
  private static final Logger LOG = LoggerFactory.getLogger(HealthCheckImplementation.class);
  private static final int MegaBytes = (1024 * 1024);

  private static final Gson GSON = new GsonBuilder().create();
  private final CConfiguration cConf;

  @Inject
  public HealthCheckImplementation(CConfiguration cConf) throws MalformedURLException {
    this.cConf = cConf;
  }

  private static String getHeapDump() throws IOException {
    JsonObject heapInfo = new JsonObject();
    long freeMemory;
    long totalMemory;
    long maxMemory;


    freeMemory = Runtime.getRuntime().freeMemory() / MegaBytes;
    totalMemory = Runtime.getRuntime().totalMemory() / MegaBytes;
    maxMemory = Runtime.getRuntime().maxMemory() / MegaBytes;

    long usedMemory = maxMemory - freeMemory;
    heapInfo.addProperty("freeMemory: MB", freeMemory + "MB");
    heapInfo.addProperty("totalMemory: MB", totalMemory + "MB");
    heapInfo.addProperty("usedMemory: MB", usedMemory + "MB");
    heapInfo.addProperty("maxMemory: MB", maxMemory + "MB");

    return GSON.toJson(heapInfo);
  }

  private static String getThreadDump() throws IOException {
    StringBuilder threadDump = new StringBuilder(System.lineSeparator());
    ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    ThreadInfo[] threadInfos = threadMXBean.dumpAllThreads(true, true);
    for (ThreadInfo threadInfo : threadInfos) {
      threadDump.append(threadInfo);
    }
    return threadDump.toString();
  }

  public HealthCheckResponse collect(String serviceName) {
    String serviceNameWithPod = serviceName + ".pod";
    String serviceNameWithNode = serviceName + ".node";
    String serviceNameWithEvent = serviceName + ".event";
    String podInfo = cConf.get(serviceNameWithPod);
    String nodeInfo = cConf.get(serviceNameWithNode);
    String eventInfo = cConf.get(serviceNameWithEvent);
    JsonArray podInfoList = GSON.fromJson(cConf.get(podInfo), JsonArray.class);
    JsonArray nodeInfoList = GSON.fromJson(cConf.get(nodeInfo), JsonArray.class);
    JsonArray eventInfoList = GSON.fromJson(cConf.get(eventInfo), JsonArray.class);

    String threadDump = "";
    String heapDump = "";
    try {
      heapDump = getHeapDump();
      threadDump = getThreadDump();
    } catch (IOException e) {
      LOG.error("Can not obtain client", e);
    }
    return HealthCheckResponse.named("Health check with data")
      .withData("podList", GSON.toJson(podInfoList))
      .withData("nodeList", GSON.toJson(nodeInfoList))
      .withData("eventList", GSON.toJson(eventInfoList))
      .withData("heapDump", heapDump)
      .withData("threadDump", threadDump)
      .up()
      .build();
  }
}

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

package io.cdap.cdap.implementation;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;

public class HealthCheckImplementation {
  private static final Logger LOG = LoggerFactory.getLogger(HealthCheckImplementation.class);
  private static final int MEGABYTES = (1024 * 1024);

  private static final Gson GSON = new GsonBuilder().create();

  public HealthCheckResponse collect() {
    String threadDump = "";
    String heapDump = "";
    try {
      heapDump = getHeapDump();
      threadDump = getThreadDump();
    } catch (IOException e) {
      LOG.error("Can not obtain client", e);
    }
    return HealthCheckResponse.named("Health check with data")
      .withData("heapDump", heapDump)
      .withData("threadDump", threadDump)
      .up()
      .build();
  }

  private static String getHeapDump() throws IOException {
    JsonObject heapInfo = new JsonObject();
    long freeMemory;
    long totalMemory;
    long maxMemory;


    freeMemory = Runtime.getRuntime().freeMemory() / MEGABYTES;
    totalMemory = Runtime.getRuntime().totalMemory() / MEGABYTES;
    maxMemory = Runtime.getRuntime().maxMemory() / MEGABYTES;

    long usedMemory = maxMemory - freeMemory;
    heapInfo.addProperty("freeMemory", String.format("%sMB", freeMemory));
    heapInfo.addProperty("totalMemory", String.format("%sMB", totalMemory));
    heapInfo.addProperty("usedMemory", String.format("%sMB", usedMemory));
    heapInfo.addProperty("maxMemory", String.format("%sMB", maxMemory));

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
}

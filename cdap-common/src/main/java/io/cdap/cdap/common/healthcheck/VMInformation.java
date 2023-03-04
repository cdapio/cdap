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

package io.cdap.cdap.common.healthcheck;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;

/**
 * A container class to hold memory and threads information of the JVM.
 */
public final class VMInformation {

  private final MemoryUsage heapMemoryUsage;
  private final MemoryUsage nonHeapMemoryUsage;
  private final String threads;

  /**
   * Creates a {@link VMInformation} based on the current state of the JVM.
   */
  public static VMInformation collect() {
    MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();

    return new VMInformation(memoryMXBean.getHeapMemoryUsage(),
        memoryMXBean.getNonHeapMemoryUsage(), getThreadDump());
  }

  private VMInformation(MemoryUsage heapMemoryUsage, MemoryUsage nonHeapMemoryUsage,
      String threads) {
    this.heapMemoryUsage = heapMemoryUsage;
    this.nonHeapMemoryUsage = nonHeapMemoryUsage;
    this.threads = threads;
  }

  public MemoryUsage getHeapMemoryUsage() {
    return heapMemoryUsage;
  }

  public MemoryUsage getNonHeapMemoryUsage() {
    return nonHeapMemoryUsage;
  }

  public String getThreads() {
    return threads;
  }

  private static String getThreadDump() {
    StringBuilder threadDump = new StringBuilder();
    ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    ThreadInfo[] threadInfos = threadMXBean.dumpAllThreads(true, true);
    for (ThreadInfo threadInfo : threadInfos) {
      threadDump.append(threadInfo);
    }
    return threadDump.toString();
  }
}

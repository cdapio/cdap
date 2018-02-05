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

package co.cask.cdap.proto.ops;

import java.util.List;

/**
 * Represents a summary of an existing dashboard view record in an HTTP response.
 */
public class ExistingDashboardSummaryRecord extends DashboardSummaryRecord {
  private final int maxMemoryAvailable;
  private final int clusterMemoryUsage;
  private final int maxCoreAvailable;
  private final int clusterCoreUsage;

  public ExistingDashboardSummaryRecord(long time,
                                        int maxMemoryAvailable, int clusterMemoryUsage,
                                        int maxCoreAvailable, int clusterCoreUsage,
                                        List<NamespaceSummary> namespaceSummaries) {
    super(time, namespaceSummaries);
    this.maxMemoryAvailable = maxMemoryAvailable;
    this.clusterMemoryUsage = clusterMemoryUsage;
    this.maxCoreAvailable = maxCoreAvailable;
    this.clusterCoreUsage = clusterCoreUsage;
  }

  public int getMaxMemoryAvailable() {
    return maxMemoryAvailable;
  }

  public int getClusterMemoryUsage() {
    return clusterMemoryUsage;
  }

  public int getMaxCoreAvailable() {
    return maxCoreAvailable;
  }

  public int getClusterCoreUsage() {
    return clusterCoreUsage;
  }

  /**
   * A summary of the existing program runs in a namespace.
   */
  public static class ExistingNamespaceSummary extends NamespaceSummary {
    private final int running;
    private final int successful;
    private final int failed;
    private final int manually;
    private final int triggered;
    private final int namespaceMemoryUsage;
    private final int namespaceCoreUsage;

    public ExistingNamespaceSummary(String namespace, int running, int successful,
                                    int failed, int manually, int scheduled, int triggered,
                                    int namespaceMemoryUsage, int namespaceCoreUsage) {
      super(namespace, scheduled);
      this.running = running;
      this.successful = successful;
      this.failed = failed;
      this.manually = manually;
      this.triggered = triggered;
      this.namespaceMemoryUsage = namespaceMemoryUsage;
      this.namespaceCoreUsage = namespaceCoreUsage;
    }

    public int getRunning() {
      return running;
    }

    public int getSuccessful() {
      return successful;
    }

    public int getFailed() {
      return failed;
    }

    public int getManually() {
      return manually;
    }

    public int getTriggered() {
      return triggered;
    }

    public int getNamespaceMemoryUsage() {
      return namespaceMemoryUsage;
    }

    public int getNamespaceCoreUsage() {
      return namespaceCoreUsage;
    }
  }
}

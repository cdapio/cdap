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
 * Represents a summary of the dashboard view in an HTTP response.
 */
public class DashboardSummaryRecord {
  private final long time;
  private final List<NamespaceSummary> namespaceSummaries;

  public DashboardSummaryRecord(long time, List<NamespaceSummary> namespaceSummaries) {
    this.time = time;
    this.namespaceSummaries = namespaceSummaries;
  }

  public long getTime() {
    return time;
  }

  public List<NamespaceSummary> getNamespaceSummaries() {
    return namespaceSummaries;
  }

  /**
   * A summary of the program runs in a namespace.
   */
  public static class NamespaceSummary {
    private final String namespace;
    private final int scheduled;

    public NamespaceSummary(String namespace, int scheduled) {
      this.namespace = namespace;
      this.scheduled = scheduled;
    }

    public String getNamespace() {
      return namespace;
    }

    public int getScheduled() {
      return scheduled;
    }
  }
}

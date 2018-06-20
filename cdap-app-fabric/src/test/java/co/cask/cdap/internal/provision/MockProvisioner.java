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

package co.cask.cdap.internal.provision;

import co.cask.cdap.proto.provisioner.ProvisionerInfo;
import co.cask.cdap.proto.provisioner.ProvisionerPropertyValue;
import co.cask.cdap.runtime.spi.provisioner.Cluster;
import co.cask.cdap.runtime.spi.provisioner.ClusterStatus;
import co.cask.cdap.runtime.spi.provisioner.ProgramRun;
import co.cask.cdap.runtime.spi.provisioner.Provisioner;
import co.cask.cdap.runtime.spi.provisioner.ProvisionerContext;
import co.cask.cdap.runtime.spi.provisioner.ProvisionerSpecification;
import co.cask.cdap.runtime.spi.provisioner.RetryableProvisionException;
import com.google.inject.Singleton;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Provisioner for unit tests. Has the same spec as the default yarn provisioner.
 */
@Singleton
public class MockProvisioner implements Provisioner {
  public static final String NAME = "yarn";
  public static final String FAIL_RETRYABLY_EVERY_N = "fail.retryably.every.n";
  public static final String FAIL_CREATE = "fail.create";
  public static final String FAIL_INIT = "fail.init";
  public static final String FAIL_GET = "fail.get";
  public static final String FAIL_DELETE = "fail.delete";
  public static final String FIRST_CLUSTER_STATUS = "first.cluster.status";
  private static final ProvisionerSpecification SPEC = new ProvisionerSpecification(
    "yarn", "Default YARN Provisioner",
    "Runs programs on the CDAP master cluster. Does not provision any resources.",
    new HashMap<>());
  private final AtomicInteger callCount;
  private final Set<ProgramRun> seenRuns;

  public MockProvisioner() {
    this.callCount = new AtomicInteger(0);
    this.seenRuns = new HashSet<>();
  }

  @Override
  public ProvisionerSpecification getSpec() {
    return SPEC;
  }

  @Override
  public void validateProperties(Map<String, String> properties) {
    // no-op
  }

  @Override
  public Cluster createCluster(ProvisionerContext context) throws RetryableProvisionException {
    failIfConfigured(context, FAIL_CREATE);
    failRetryablyEveryN(context);
    return new Cluster(context.getProgramRun().getRun(), ClusterStatus.CREATING,
                       Collections.emptyList(), Collections.emptyMap());
  }

  @Override
  public void initializeCluster(ProvisionerContext context, Cluster cluster) throws Exception {
    failIfConfigured(context, FAIL_INIT);
    failRetryablyEveryN(context);
  }

  @Override
  public Cluster getClusterDetail(ProvisionerContext context, Cluster cluster) throws RetryableProvisionException {
    failIfConfigured(context, FAIL_GET);
    failRetryablyEveryN(context);
    ClusterStatus status = cluster.getStatus();
    ClusterStatus newStatus;
    String firstClusterStatus = context.getProperties().get(FIRST_CLUSTER_STATUS);
    if (seenRuns.add(context.getProgramRun()) && firstClusterStatus != null) {
      newStatus = ClusterStatus.valueOf(firstClusterStatus);
    } else {
      switch (status) {
        case CREATING:
          newStatus = ClusterStatus.RUNNING;
          break;
        case DELETING:
          newStatus = ClusterStatus.NOT_EXISTS;
          break;
        default:
          newStatus = status;
          break;
      }
    }
    return new Cluster(cluster, newStatus);
  }

  @Override
  public void deleteCluster(ProvisionerContext context, Cluster cluster) throws RetryableProvisionException {
    failIfConfigured(context, FAIL_DELETE);
    failRetryablyEveryN(context);
  }

  // throws a RetryableProvisionException every other time this is called
  private void failRetryablyEveryN(ProvisionerContext context) throws RetryableProvisionException {
    String nStr = context.getProperties().get(FAIL_RETRYABLY_EVERY_N);
    if (nStr != null) {
      int n = Integer.parseInt(nStr);
      if (callCount.getAndIncrement() % n == 0) {
        throw new RetryableProvisionException(String.format("Failing call #%d", callCount.get() - 1));
      }
    }
  }

  // throws a RuntimeException if the specified key evaluates to true
  private void failIfConfigured(ProvisionerContext context, String key) {
    boolean shouldFail = Boolean.parseBoolean(context.getProperties().get(key));
    if (shouldFail) {
      throw new RuntimeException();
    }
  }

  /**
   * Builds properties supported by the MockProvisioner.
   */
  public static class PropertyBuilder {
    private Integer failRetryablyEveryN;
    private boolean failCreate = false;
    private boolean failGet = false;
    private boolean failInit = false;
    private boolean failDelete = false;
    private ClusterStatus firstClusterStatus;

    /**
     * Configures the provisioner to fail in a retryable fashion every Nth method call.
     */
    public PropertyBuilder failRetryablyEveryN(int n) {
      failRetryablyEveryN = n;
      return this;
    }

    /**
     * Configures the provisioner to fail the create call.
     */
    public PropertyBuilder failCreate() {
      failCreate = true;
      return this;
    }

    /**
     * Configures the provisioner to fail the get call.
     */
    public PropertyBuilder failGet() {
      failGet = true;
      return this;
    }

    /**
     * Configures the provisioner to fail the init call.
     */
    public PropertyBuilder failInit() {
      failInit = true;
      return this;
    }

    /**
     * Configures the provisioner to fail the delete call.
     */
    public PropertyBuilder failDelete() {
      failDelete = true;
      return this;
    }

    /**
     * Configures the provisioner to return the specified status as the cluster status the first time getCluster is
     * called for each program run.
     */
    public PropertyBuilder setFirstClusterStatus(ClusterStatus status) {
      firstClusterStatus = status;
      return this;
    }

    public ProvisionerInfo build() {
      List<ProvisionerPropertyValue> properties = new ArrayList<>();
      properties.add(new ProvisionerPropertyValue(FAIL_CREATE, String.valueOf(failCreate), true));
      properties.add(new ProvisionerPropertyValue(FAIL_GET, String.valueOf(failGet), true));
      properties.add(new ProvisionerPropertyValue(FAIL_INIT, String.valueOf(failInit), true));
      properties.add(new ProvisionerPropertyValue(FAIL_DELETE, String.valueOf(failDelete), true));
      if (failRetryablyEveryN != null) {
        properties.add(new ProvisionerPropertyValue(FAIL_RETRYABLY_EVERY_N, String.valueOf(failRetryablyEveryN), true));
      }
      if (firstClusterStatus != null) {
        properties.add(new ProvisionerPropertyValue(FIRST_CLUSTER_STATUS, firstClusterStatus.name(), true));
      }
      return new ProvisionerInfo(NAME, properties);
    }
  }
}

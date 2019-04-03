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

import co.cask.cdap.proto.profile.Profile;
import co.cask.cdap.proto.provisioner.ProvisionerInfo;
import co.cask.cdap.proto.provisioner.ProvisionerPropertyValue;
import co.cask.cdap.runtime.spi.provisioner.Capabilities;
import co.cask.cdap.runtime.spi.provisioner.Cluster;
import co.cask.cdap.runtime.spi.provisioner.ClusterStatus;
import co.cask.cdap.runtime.spi.provisioner.PollingStrategies;
import co.cask.cdap.runtime.spi.provisioner.PollingStrategy;
import co.cask.cdap.runtime.spi.provisioner.ProgramRun;
import co.cask.cdap.runtime.spi.provisioner.Provisioner;
import co.cask.cdap.runtime.spi.provisioner.ProvisionerContext;
import co.cask.cdap.runtime.spi.provisioner.ProvisionerSpecification;
import co.cask.cdap.runtime.spi.provisioner.RetryableProvisionException;
import com.google.inject.Singleton;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Provisioner for unit tests. Has the same spec as the native provisioner.
 */
@Singleton
public class MockProvisioner implements Provisioner {
  public static final String NAME = Profile.NATIVE_NAME;
  public static final String FAIL_RETRYABLY_EVERY_N = "fail.retryably.every.n";
  public static final String FAIL_CREATE = "fail.create";
  public static final String FAIL_INIT = "fail.init";
  public static final String FAIL_GET = "fail.get";
  public static final String FAIL_DELETE = "fail.delete";
  public static final String FIRST_CLUSTER_STATUS = "first.cluster.status";
  public static final String WAIT_CREATE_MS = "wait.create";
  public static final String WAIT_DELETE_MS = "wait.delete";
  private static final ProvisionerSpecification SPEC = new ProvisionerSpecification(
    NAME, "Native", "Runs programs on the CDAP master cluster. Does not provision any resources.");
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
  public Cluster createCluster(ProvisionerContext context) throws RetryableProvisionException, InterruptedException {
    failIfConfigured(context, FAIL_CREATE);
    failRetryablyEveryN(context);
    waitIfConfigured(context, WAIT_CREATE_MS);
    return new Cluster(context.getProgramRun().getRun(), ClusterStatus.CREATING,
                       Collections.emptyList(), Collections.emptyMap());
  }

  @Override
  public void initializeCluster(ProvisionerContext context, Cluster cluster) {
    failIfConfigured(context, FAIL_INIT);
  }

  @Override
  public ClusterStatus getClusterStatus(ProvisionerContext context,
                                        Cluster cluster) throws RetryableProvisionException {
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
    return newStatus;
  }

  @Override
  public Cluster getClusterDetail(ProvisionerContext context, Cluster cluster) throws RetryableProvisionException {
    return new Cluster(cluster, getClusterStatus(context, cluster));
  }

  @Override
  public void deleteCluster(ProvisionerContext context, Cluster cluster)
    throws RetryableProvisionException, InterruptedException {
    failIfConfigured(context, FAIL_DELETE);
    failRetryablyEveryN(context);
    waitIfConfigured(context, WAIT_DELETE_MS);
  }

  @Override
  public PollingStrategy getPollingStrategy(ProvisionerContext context, Cluster cluster) {
    // retry immediately in unit tests
    return PollingStrategies.fixedInterval(0, TimeUnit.MILLISECONDS);
  }

  @Override
  public Capabilities getCapabilities() {
    return NativeProvisioner.SYSTEM_DATASETS;
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

  private void waitIfConfigured(ProvisionerContext context, String key) throws InterruptedException {
    long dur = context.getProperties().containsKey(key) ? Long.parseLong(context.getProperties().get(key)) : -1L;
    TimeUnit.MILLISECONDS.sleep(dur);
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
    private long waitCreateMillis = -1L;
    private long waitDeleteMillis = -1L;
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
     * Configures the provisioner to wait for some time before creating a cluster.
     */
    public PropertyBuilder waitCreate(long dur, TimeUnit unit) {
      waitCreateMillis = TimeUnit.MILLISECONDS.convert(dur, unit);
      return this;
    }

    /**
     * Configures the provisioner to wait for some time before deleting a cluster.
     */
    public PropertyBuilder waitDelete(long dur, TimeUnit unit) {
      waitDeleteMillis = TimeUnit.MILLISECONDS.convert(dur, unit);
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
      properties.add(new ProvisionerPropertyValue(FAIL_CREATE, Boolean.toString(failCreate), true));
      properties.add(new ProvisionerPropertyValue(FAIL_GET, Boolean.toString(failGet), true));
      properties.add(new ProvisionerPropertyValue(FAIL_INIT, Boolean.toString(failInit), true));
      properties.add(new ProvisionerPropertyValue(FAIL_DELETE, Boolean.toString(failDelete), true));
      properties.add(new ProvisionerPropertyValue(WAIT_CREATE_MS, Long.toString(waitCreateMillis), true));
      properties.add(new ProvisionerPropertyValue(WAIT_DELETE_MS, Long.toString(waitDeleteMillis), true));
      if (failRetryablyEveryN != null) {
        properties.add(new ProvisionerPropertyValue(FAIL_RETRYABLY_EVERY_N,
                                                    Integer.toString(failRetryablyEveryN), true));
      }
      if (firstClusterStatus != null) {
        properties.add(new ProvisionerPropertyValue(FIRST_CLUSTER_STATUS, firstClusterStatus.name(), true));
      }
      return new ProvisionerInfo(NAME, properties);
    }
  }
}

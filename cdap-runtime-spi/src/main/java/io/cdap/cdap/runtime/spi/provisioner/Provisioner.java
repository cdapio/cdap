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

package io.cdap.cdap.runtime.spi.provisioner;

import io.cdap.cdap.runtime.spi.runtimejob.RuntimeJobManager;

import java.util.Map;
import java.util.Optional;

/**
 * A Provisioner is responsible for creating and deleting clusters for program runs. Each method may be retried
 * in case of a failure, so must be implemented in an idempotent way.
 */
public interface Provisioner {

  /**
   * Return the specification for the provisioner.
   *
   * @return the provisioner specification
   */
  ProvisionerSpecification getSpec();

  /**
   * Initialize the provisioner. This method is guaranteed to be called before any other method is called.
   * It will only be called once for the lifetime of the provisioner.
   *
   * If this method throws a runtime exception, the provisioner will not be available for use.
   * This can cause clusters that were created by the provisioner to be stranded in an orphaned state.
   * As such, provisioners should be careful to only throw exceptions when there is absolutely nothing that can be done.
   * For example, if an optional property is set to an invalid value, that property should be ignored.
   * An exception should not be thrown in that case.
   *
   * @param systemContext the system context that can be used to initialize the provisioner
   */
  default void initialize(ProvisionerSystemContext systemContext) {
    // no-op
  }

  /**
   * Check that the specified properties are valid. If they are not, an IllegalArgumentException should be thrown.
   *
   * @param properties properties to validate
   * @throws IllegalArgumentException if the properties are invalid
   */
  void validateProperties(Map<String, String> properties);

  /**
   * Request to create a cluster. The cluster does not have to be operational before the method returns,
   * but it must be at least in the processes of being created. Must be implemented in an idempotent way,
   * otherwise there may be resource leaks. This means the implementation must first check if the cluster exists
   * before trying to create it, otherwise multiple clusters may be created for a single program run.
   * The cluster returned will be passed to subsequent calls.
   *
   * @param context provisioner context
   * @return information about the cluster
   * @throws RetryableProvisionException if the operation failed, but may succeed on a retry
   * @throws Exception if the operation failed in a non-retryable fashion
   */
  Cluster createCluster(ProvisionerContext context) throws Exception;

  /**
   * Get the status of the cluster. If it does not exist, {@link ClusterStatus#NOT_EXISTS} should be returned.
   * This is used to poll for status after {@link #createCluster(ProvisionerContext)} or
   * {@link #deleteCluster(ProvisionerContext, Cluster)} is called.
   *
   * @param context provisioner context
   * @param cluster the cluster to get the status of
   * @return the status of the cluster
   * @throws RetryableProvisionException if the operation failed, but may succeed on a retry
   * @throws Exception if the operation failed in a non-retryable fashion
   */
  ClusterStatus getClusterStatus(ProvisionerContext context, Cluster cluster) throws Exception;

  /**
   * Get details about the cluster. If the cluster does not exist, a cluster with status
   * {@link ClusterStatus#NOT_EXISTS} should be returned. This is called after a cluster has been created, but before
   * it has been initialized.
   *
   * @param context provisioner context
   * @param cluster the cluster to get
   * @return details about the cluster
   * @throws RetryableProvisionException if the operation failed, but may succeed on a retry
   * @throws Exception if the operation failed in a non-retryable fashion
   */
  Cluster getClusterDetail(ProvisionerContext context, Cluster cluster) throws Exception;

  /**
   * Responsible for any additional initialization steps for the cluster.
   * This method will be called after the cluster is created and goes into RUNNING status.
   * This method might get called multiple times in case of failure, hence it is advised to
   * have it be implemented in an idempotent way.
   *
   * By default this method is an no-op.
   *
   * @param context provisioner context
   * @param cluster the cluster to operate on
   * @throws RetryableProvisionException if the operation failed, but may succeed on a retry
   * @throws Exception if the operation failed in a non-retryable fashion
   */
  default void initializeCluster(ProvisionerContext context, Cluster cluster) throws Exception {
    // no-op
  }

  /**
   * Request to delete a cluster. The cluster does not have to be deleted before the method returns, but it must
   * at least be in the process of being deleted. Must be implemented in an idempotent way.
   *
   * @param context provisioner context
   * @param cluster the cluster to delete
   * @throws RetryableProvisionException if the operation failed, but may succeed on a retry
   * @throws Exception if the operation failed in a non-retryable fashion
   * @deprecated Since 6.2.0. Implements the {@link #deleteClusterWithStatus(ProvisionerContext, Cluster)} as well.
   */
  @Deprecated
  void deleteCluster(ProvisionerContext context, Cluster cluster) throws Exception;

  /**
   * Request to delete a cluster. The cluster does not have to be deleted before the method returns, but it must
   * at least be in the process of being deleted. Must be implemented in an idempotent way.
   *
   * @param context provisioner context
   * @param cluster the cluster to delete
   * @return the {@link ClusterStatus} after the delete cluster operation
   * @throws RetryableProvisionException if the operation failed, but may succeed on a retry
   * @throws Exception if the operation failed in a non-retryable fashion
   */
  default ClusterStatus deleteClusterWithStatus(ProvisionerContext context, Cluster cluster) throws Exception {
    deleteCluster(context, cluster);
    return ClusterStatus.DELETING;
  }

  /**
   * Get the {@link PollingStrategy} to use when polling for cluster creation or deletion. The cluster status is
   * guaranteed to be {@link ClusterStatus#CREATING} or {@link ClusterStatus#DELETING}.
   *
   * @param context provisioner context
   * @param cluster the cluster to poll status for
   */
  PollingStrategy getPollingStrategy(ProvisionerContext context, Cluster cluster);

  /**
   * Returns {@link Capabilities} of this provisioner. Capabilities allow plugins requiring special Requirements
   * to be run in the provisioner if all the requirements are met. If a program requires a requirement that is not
   * supported by the provisioner, the platform will fail the program run before provisioning begins.
   *
   * @return {@link Capabilities} of this provisioner
   */
  Capabilities getCapabilities();

  /**
   * Returns {@link RuntimeJobManager} to launch and manage runtime job. If the optional is empty, default
   * implementation will use ssh to launch and manage jobs.
   *
   * @param context provisioner context
   * @return optional runtime job manager, if it is empty, ssh will be used to launch and manage jobs.
   */
  default Optional<RuntimeJobManager> getRuntimeJobManager(ProvisionerContext context) {
    return Optional.empty();
  }
}

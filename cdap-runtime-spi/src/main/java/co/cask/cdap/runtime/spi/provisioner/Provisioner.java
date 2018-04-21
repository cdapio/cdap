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

package co.cask.cdap.runtime.spi.provisioner;

import java.util.Map;

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
   *
   * @param context provisioner context
   * @return information about the cluster
   * @throws RetryableProvisionException if the operation failed, but may succeed on a retry
   * @throws Exception if the operation failed in a non-retryable fashion
   */
  Cluster createCluster(ProvisionerContext context) throws Exception;

  /**
   * Get details about the cluster. If the cluster does not exist, a cluster with status
   * {@link ClusterStatus#NOT_EXISTS} should be returned.
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
   */
  void deleteCluster(ProvisionerContext context, Cluster cluster) throws Exception;

}

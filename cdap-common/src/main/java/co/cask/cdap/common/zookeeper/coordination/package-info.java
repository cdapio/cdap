/*
 * Copyright © 2014 Cask Data, Inc.
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

/**
 * This package contains classes for performing coordination of resource allocation.
 *
 * The two important classes are {@link co.cask.cdap.common.zookeeper.coordination.ResourceCoordinator}
 * and {@link co.cask.cdap.common.zookeeper.coordination.ResourceCoordinatorClient}.
 *
 * The {@link co.cask.cdap.common.zookeeper.coordination.ResourceCoordinator}
 * is expected to have single instance running and is responsible for matching
 * {@link co.cask.cdap.common.zookeeper.coordination.PartitionReplica} to
 * {@link org.apache.twill.discovery.Discoverable}, based on the specification as described by
 * {@link co.cask.cdap.common.zookeeper.coordination.ResourceRequirement}. The actual strategy used for
 * the matching is controlled by {@link co.cask.cdap.common.zookeeper.coordination.AssignmentStrategy}.
 *
 * The {@link co.cask.cdap.common.zookeeper.coordination.ResourceCoordinatorClient} is for the
 * {@link org.apache.twill.discovery.Discoverable}s who are interested in handling changes in resource
 * assignment. It is also served as a admin interface for create/update/delete of
 * {@link co.cask.cdap.common.zookeeper.coordination.ResourceRequirement}.
 *
 * The coordination service is designed in a way that the interruption of
 * {@link co.cask.cdap.common.zookeeper.coordination.ResourceCoordinator} availability will only affect
 * reassignment, triggered by new/update of resource requirement or changes in Discoverable. All
 * current resource assignment would not be affected, meaning all clients should still be able to operate
 * normally as long as they are up and running.
 */
package co.cask.cdap.common.zookeeper.coordination;

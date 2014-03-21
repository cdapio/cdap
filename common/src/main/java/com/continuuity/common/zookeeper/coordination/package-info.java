/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */

/**
 * This package contains classes for performing coordination of resource allocation.
 *
 * The two important classes are {@link com.continuuity.common.zookeeper.coordination.ResourceCoordinator}
 * and {@link com.continuuity.common.zookeeper.coordination.ResourceCoordinatorClient}.
 *
 * The {@link com.continuuity.common.zookeeper.coordination.ResourceCoordinator}
 * is expected to have single instance running and is responsible for matching
 * {@link com.continuuity.common.zookeeper.coordination.PartitionReplica} to
 * {@link org.apache.twill.discovery.Discoverable}, based on the specification as described by
 * {@link com.continuuity.common.zookeeper.coordination.ResourceRequirement}. The actual strategy used for
 * the matching is controlled by {@link com.continuuity.common.zookeeper.coordination.AssignmentStrategy}.
 *
 * The {@link com.continuuity.common.zookeeper.coordination.ResourceCoordinatorClient} is for the
 * {@link org.apache.twill.discovery.Discoverable}s who are interested in handling changes in resource
 * assignment. It is also served as a admin interface for create/update/delete of
 * {@link com.continuuity.common.zookeeper.coordination.ResourceRequirement}.
 *
 * The coordination service is designed in a way that the interruption of
 * {@link com.continuuity.common.zookeeper.coordination.ResourceCoordinator} availability will only affect
 * reassignment, triggered by new/update of resource requirement or changes in Discoverable. All
 * current resource assignment would not be affected, meaning all clients should still be able to operate
 * normally as long as they are up and running.
 */
package com.continuuity.common.zookeeper.coordination;

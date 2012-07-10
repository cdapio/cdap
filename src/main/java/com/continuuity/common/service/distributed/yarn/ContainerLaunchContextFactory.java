package com.continuuity.common.service.distributed.yarn;

import com.continuuity.common.service.distributed.TaskSpecification;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.util.Records;

/**
 * Factory that is used for constructing YARN objects from the <code>ContainerGroupParameter</code>
 */
public class ContainerLaunchContextFactory {
  private final Resource clusterMin;
  private final Resource clusterMax;

  public ContainerLaunchContextFactory(Resource clusterMin, Resource clusterMax) {
    this.clusterMin = clusterMin;
    this.clusterMax = clusterMax;
  }

  public ContainerLaunchContext create(TaskSpecification specification) {
    ContainerLaunchContext clc = Records.newRecord(ContainerLaunchContext.class);
    clc.setCommands(specification.getCommands());
    clc.setEnvironment(specification.getEnvironment());
    clc.setLocalResources(specification.getNamedLocalResources());
    clc.setResource(specification.getContainerResource(clusterMin, clusterMax));
    clc.setUser(specification.getUser());
    return clc;
  }

  public ResourceRequest createResourceRequest(TaskSpecification specification) {
    ResourceRequest req = Records.newRecord(ResourceRequest.class);
    req.setCapability(specification.getContainerResource(clusterMin, clusterMax));
    req.setPriority(createPriority(specification.getPriority()));
    req.setNumContainers(specification.getNumInstances());
    req.setHostName("*"); /** right now we don't care where the containers are started. */
    return req;
  }

  public static Priority createPriority(int priority) {
    Priority p = Records.newRecord(Priority.class);
    p.setPriority(priority);
    return p;
  }
}

package com.continuuity.common.distributedservice.yarn;

import com.continuuity.common.distributedservice.ContainerGroupSpecification;
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

  public ContainerLaunchContext create(ContainerGroupSpecification parameters) {
    ContainerLaunchContext clc = Records.newRecord(ContainerLaunchContext.class);
    clc.setCommands(parameters.getCommands());
    clc.setEnvironment(parameters.getEnvironment());
    //clc.setLocalResources(parameters.getLocalResources());
    clc.setResource(parameters.getContainerResource(clusterMin, clusterMax));
    clc.setUser(parameters.getUser());
    return clc;
  }

  public ResourceRequest createResourceRequest(ContainerGroupSpecification parameters) {
    ResourceRequest req = Records.newRecord(ResourceRequest.class);
    req.setCapability(parameters.getContainerResource(clusterMin, clusterMax));
    req.setPriority(createPriority(parameters.getPriority()));
    req.setNumContainers(parameters.getNumInstances());
    req.setHostName("*"); /** right now we don't care where the containers are started. */
    return req;
  }

  public static Priority createPriority(int priority) {
    Priority p = Records.newRecord(Priority.class);
    p.setPriority(priority);
    return p;
  }
}

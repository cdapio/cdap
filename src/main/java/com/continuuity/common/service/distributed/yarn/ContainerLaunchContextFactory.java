package com.continuuity.common.service.distributed.yarn;

import com.continuuity.common.service.distributed.TaskSpecification;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Factory that is used for constructing YARN objects from the <code>ContainerGroupParameter</code>
 */
public class ContainerLaunchContextFactory {
  private static final Logger Log = LoggerFactory.getLogger(ContainerLaunchContextFactory.class);
  private final Resource clusterMin;
  private final Resource clusterMax;

  public ContainerLaunchContextFactory(Resource clusterMin, Resource clusterMax) {
    this.clusterMin = clusterMin;
    this.clusterMax = clusterMax;
  }

  public ContainerLaunchContext create(TaskSpecification specification) {
    ContainerLaunchContext clc = Records.newRecord(ContainerLaunchContext.class);
    Log.info("Cluster Min {}, Cluster Max {}", clusterMin, clusterMax);
    clc.setResource(specification.getContainerResource(clusterMin, clusterMax));
    for(String cmd : specification.getCommands()) {
      Log.info("Command : {}", cmd);
    }
    clc.setCommands(specification.getCommands());
    clc.setUser(specification.getUser());
    clc.setLocalResources(specification.getNamedLocalResources());
    clc.setEnvironment(specification.getEnvironment());
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

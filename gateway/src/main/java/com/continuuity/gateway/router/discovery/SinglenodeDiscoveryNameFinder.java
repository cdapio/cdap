package com.continuuity.gateway.router.discovery;

import com.continuuity.app.program.Type;
import com.continuuity.app.runtime.ProgramRuntimeService;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Finds the discovery service name for a given program when it is running in single node.
 */
public class SinglenodeDiscoveryNameFinder implements DiscoveryNameFinder {
  private static final Logger LOG = LoggerFactory.getLogger(SinglenodeDiscoveryNameFinder.class);

  private final ProgramRuntimeService programRuntimeService;

  @Inject
  public SinglenodeDiscoveryNameFinder(ProgramRuntimeService programRuntimeService) {
    this.programRuntimeService = programRuntimeService;
  }

  @Override
  public String findDiscoveryServiceName(Type type, String name) {
    String typeStr = type.name().toLowerCase() + ".";

    for (ProgramRuntimeService.RuntimeInfo info : programRuntimeService.list(type).values()) {
      LOG.debug("Got application name {}{}.{}.{}", typeStr, info.getProgramId().getAccountId(),
                info.getProgramId().getApplicationId(), info.getProgramId().getId());

      if (info.getProgramId().getId().equals(name)) {
        return String.format("%s%s.%s.%s", typeStr, info.getProgramId().getAccountId(),
                             info.getProgramId().getApplicationId(), name);
      }
    }

    return name;
  }
}

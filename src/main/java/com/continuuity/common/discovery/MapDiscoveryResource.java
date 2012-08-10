package com.continuuity.common.discovery;

import com.netflix.curator.x.discovery.server.rest.DiscoveryContext;
import com.netflix.curator.x.discovery.server.rest.DiscoveryResource;

import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.ext.ContextResolver;

/**
 * Maps the standard discovery resource for service discovery.
 */
@Path("/")
public class MapDiscoveryResource extends DiscoveryResource<ServicePayload> {
  public MapDiscoveryResource(@Context
                              ContextResolver<
                                DiscoveryContext<ServicePayload>
                              > resolver) {
    super(resolver.getContext(DiscoveryContext.class));
  }
}

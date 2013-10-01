package com.continuuity.gateway.v2.handlers.v2;

import com.continuuity.common.http.core.HandlerContext;
import com.continuuity.gateway.auth.GatewayAuthenticator;
import com.continuuity.metadata.MetaDataStore;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Path;

/**
 *  {@link MetadataServiceHandler} is REST interface to MDS store.
 */
@Path("/v2")
public class MetadataServiceHandler extends AuthenticatedHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(MetadataServiceHandler.class);

  @Inject
  public MetadataServiceHandler(GatewayAuthenticator authenticator) {
    super(authenticator);
  }

  @Override
  public void init(HandlerContext context) {
    LOG.info("Starting MetadataServiceHandler.");
  }

  @Override
  public void destroy(HandlerContext context) {
    LOG.info("Stopping MetadataServiceHandler.");
  }
}


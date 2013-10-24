package com.continuuity.gateway.v2.handlers.v2.dataset;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.http.core.HandlerContext;
import com.continuuity.common.http.core.HttpResponder;
import com.continuuity.data2.transaction.queue.QueueAdmin;
import com.continuuity.data2.transaction.queue.StreamAdmin;
import com.continuuity.gateway.auth.GatewayAuthenticator;
import com.continuuity.gateway.v2.handlers.v2.AuthenticatedHttpHandler;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.DELETE;
import javax.ws.rs.Path;

import static org.jboss.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;

/**
 * Handles data fabric clear calls.
 */
@Path(Constants.Gateway.GATEWAY_VERSION)
public class ClearFabricHandler extends AuthenticatedHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(ClearFabricHandler.class);

  private final QueueAdmin queueAdmin;
  private final StreamAdmin streamAdmin;

  @Inject
  public ClearFabricHandler(QueueAdmin queueAdmin, StreamAdmin streamAdmin,
                            GatewayAuthenticator authenticator) {
    super(authenticator);
    this.queueAdmin = queueAdmin;
    this.streamAdmin = streamAdmin;
  }

  @Override
  public void init(HandlerContext context) {
    LOG.info("Starting ClearFabricHandler");
    super.init(context);
  }


  @Override
  public void destroy(HandlerContext context) {
    LOG.info("Stopping ClearFabricHandler");
  }

  @DELETE
  @Path("/queues")
  public void clearQueues(HttpRequest request, final HttpResponder responder) {
    clear(request, responder, ToClear.QUEUES);
  }

  @DELETE
  @Path("/streams")
  public void clearStreams(HttpRequest request, final HttpResponder responder) {
    clear(request, responder, ToClear.STREAMS);
  }

  private static enum ToClear {
    QUEUES, STREAMS
  }

  private void clear(HttpRequest request, final HttpResponder responder, ToClear toClear) {
    try {
      getAuthenticatedAccountId(request);
      try {
        if (toClear == ToClear.QUEUES) {
          queueAdmin.dropAll();
        } else if (toClear == ToClear.STREAMS) {
          streamAdmin.dropAll();
        }
        responder.sendStatus(OK);
      } catch (Exception e) {
        LOG.trace("Exception clearing data fabric: ", e);
        responder.sendStatus(INTERNAL_SERVER_ERROR);
      }
    } catch (SecurityException e) {
      responder.sendStatus(UNAUTHORIZED);
    } catch (IllegalArgumentException e) {
      responder.sendString(BAD_REQUEST, e.getMessage());
    }  catch (Throwable e) {
      LOG.error("Caught exception", e);
      responder.sendStatus(INTERNAL_SERVER_ERROR);
    }
  }
}


package com.continuuity.gateway.v2.handlers.v2.dataset;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.http.core.HandlerContext;
import com.continuuity.common.http.core.HttpResponder;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data2.transaction.queue.QueueAdmin;
import com.continuuity.gateway.auth.GatewayAuthenticator;
import com.continuuity.gateway.v2.handlers.v2.AuthenticatedHttpHandler;
import com.continuuity.metadata.MetaDataTable;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.DELETE;
import javax.ws.rs.Path;

import static org.jboss.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.OK;

/**
 * Handles data fabric clear calls.
 */
@Path(Constants.Gateway.GATEWAY_VERSION)
public class ClearFabricHandler extends AuthenticatedHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(ClearFabricHandler.class);

  private final MetaDataTable metadataTable;
  private final QueueAdmin queueAdmin;
  private final DataSetAccessor dataSetAccessor;

  @Inject
  public ClearFabricHandler(MetaDataTable metaDataTable, QueueAdmin queueAdmin,
                            DataSetAccessor dataSetAccessor, GatewayAuthenticator authenticator) {
    super(authenticator);
    this.metadataTable = metaDataTable;
    this.queueAdmin = queueAdmin;
    this.dataSetAccessor = dataSetAccessor;
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
  @Path("/meta")
  public void clearMeta(HttpRequest request, final HttpResponder responder) {
    clear(request, responder, ToClear.META);
  }

  @DELETE
  @Path("/datasets")
  public void clearTables(HttpRequest request, final HttpResponder responder) {
    clear(request, responder, ToClear.TABLES);
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

  @DELETE
  @Path("/all")
  public void clearAll(HttpRequest request, final HttpResponder responder) {
    clear(request, responder, ToClear.ALL);
  }

  private static enum ToClear {
    DATA, META, TABLES, QUEUES, STREAMS, ALL
  }

  private void clear(HttpRequest request, final HttpResponder responder, ToClear toClear) {
    try {
      String accountId = getAuthenticatedAccountId(request);
      OperationContext context = new OperationContext(accountId);

      try {
        if (toClear == ToClear.ALL || toClear == ToClear.TABLES) {
          dataSetAccessor.dropAll(DataSetAccessor.Namespace.USER);
          // todo: do we have to drop system datasets, too?
        }
        if (toClear == ToClear.ALL || toClear == ToClear.META) {
          metadataTable.clear(context, context.getAccount(), null);
        }
        if (toClear == ToClear.ALL || toClear == ToClear.QUEUES) {
          queueAdmin.dropAll();
        }
        if (toClear == ToClear.ALL || toClear == ToClear.STREAMS) {
          // NOTE: for now we store all streams data in same queue table, TODO: fix this
          queueAdmin.dropAll();
        }

        responder.sendStatus(OK);
      } catch (Exception e) {
        LOG.trace("Exception clearing data fabric: ", e);
        responder.sendStatus(INTERNAL_SERVER_ERROR);
      }

    } catch (SecurityException e) {
      responder.sendStatus(FORBIDDEN);
    } catch (IllegalArgumentException e) {
      responder.sendStatus(BAD_REQUEST);
    }  catch (Throwable e) {
      LOG.error("Caught exception", e);
      responder.sendStatus(INTERNAL_SERVER_ERROR);
    }
  }
}


package com.continuuity.gateway.accessor;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.metrics.CMetrics;
import com.continuuity.common.metrics.MetricsHelper;
import com.continuuity.data.metadata.MetaDataEntry;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.gateway.util.NettyRestHandler;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import static com.continuuity.common.metrics.MetricsHelper.Status.BadRequest;
import static com.continuuity.common.metrics.MetricsHelper.Status.Error;
import static com.continuuity.common.metrics.MetricsHelper.Status.NoData;
import static com.continuuity.common.metrics.MetricsHelper.Status.NotFound;
import static com.continuuity.common.metrics.MetricsHelper.Status.Success;

/**
 * This is the http request handler for the metadata REST API.
 * <p/>
 * At this time it only accepts GET requests to list entries or to view an entry.
 * Examples of well-formed reqeuests:
 * <PRE>
 * list GET http://gateway:port/metadata/type/id[&encoding=name]
 * </PRE>
 */
public class MetaDataRestHandler extends NettyRestHandler {

  private static final Logger LOG = LoggerFactory
    .getLogger(MetaDataRestHandler.class);

  // the allowed methods for this handler
  Set<HttpMethod> allowedMethods = Collections.singleton(HttpMethod.GET);

  // The metrics object of the rest accessor
  private final CMetrics metrics;

  // prefix for all paths
  private String pathPrefix;

  // the accessor that started this handler
  private MetaDataRestAccessor accessor;

  // the json builder with pretty printing
  private Gson gson = new GsonBuilder().setPrettyPrinting().create();

  // helper method to parse the path. TODO move this to util
  LinkedList<String> splitPath(String path) {
    LinkedList<String> components = Lists.newLinkedList();
    StringTokenizer tok = new StringTokenizer(path, "/");
    while (tok.hasMoreElements()) {
      components.add(tok.nextToken());
    }
    return components;
  }

  /**
   * Constructor requires the accessor that created this
   *
   * @param accessor the accessor that created this
   */
  MetaDataRestHandler(MetaDataRestAccessor accessor) {
    this.accessor = accessor;
    this.metrics = accessor.getMetricsClient();
    this.pathPrefix =
      accessor.getHttpConfig().getPathPrefix() +
        accessor.getHttpConfig().getPathMiddle();
  }

  @Override
  public void messageReceived(ChannelHandlerContext context,
                              MessageEvent message) throws Exception {

    HttpRequest request = (HttpRequest) message.getMessage();
    HttpMethod method = request.getMethod();
    String uri = request.getUri();
    QueryStringDecoder decoder = new QueryStringDecoder(uri);
    String path = decoder.getPath();
    Map<String, List<String>> parameters = decoder.getParameters();

    if (LOG.isTraceEnabled()) {
      LOG.trace("Request received: " + method + " " + uri);
    }
    MetricsHelper helper = new MetricsHelper(
      this.getClass(), this.metrics, this.accessor.getMetricsQualifier());

    // only GET is supported for now
    if (method != HttpMethod.GET) {
      LOG.trace("Received a " + method + " request, which is not supported");
      respondNotAllowed(message.getChannel(), allowedMethods);
      helper.finish(BadRequest);
      return;
    }

    // is this a ping? (http://gw:port/ping) if so respond OK and done
    if ("/ping".equals(path)) {
      respondToPing(message.getChannel(), request);
      helper.finish(Success);
      return;
    }

    // check that path begins with prefix
    if (!path.startsWith(this.pathPrefix)) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Received a request with unkown path prefix " +
                    "(must be '" + this.pathPrefix + "' but received '" + path + "'.");
      }
      respondError(message.getChannel(), HttpResponseStatus.NOT_FOUND);
      helper.finish(NotFound);
      return;
    }

    try {
      // if authentication is enabled, verify an authentication token has been
      // passed and then verify the token is valid
      if (!accessor.getAuthenticator().authenticateRequest(request)) {
        respondError(message.getChannel(), HttpResponseStatus.FORBIDDEN);
        helper.finish(BadRequest);
        return;
      }
      String accountId = accessor.getAuthenticator().getAccountId(request);
      if (accountId == null || accountId.isEmpty()) {
        LOG.info("No valid account information found");
        respondError(message.getChannel(), HttpResponseStatus.FORBIDDEN);
        helper.finish(BadRequest);
        return;
      }
      OperationContext opContext = new OperationContext(accountId);

      // get the list of path components
      LinkedList<String> pathComponents = splitPath(path.substring(this.pathPrefix.length()));

      // first component must be the entry type
      if (pathComponents.isEmpty()) {
        respondBadRequest(message, request, helper, "entry type missing");
        return;
      }
      String type = pathComponents.removeFirst();

      // second component is optional, would be the entry id
      String entryId = pathComponents.pollFirst();
      helper.setMethod(entryId == null ? "list" : "view");

      // no more components allowed on path
      if (!pathComponents.isEmpty()) {
        respondBadRequest(message, request, helper, "extra components in path");
        return;
      }

      // optional parameter application
      String application = null;
      Map<String, String> filters = Maps.newHashMap();
      for (Map.Entry<String, List<String>> entry : parameters.entrySet()) {
        List<String> values = entry.getValue();
        if (values.isEmpty()) {
          continue; // should not happen
        }
        String value = values.get(0);
        String key = entry.getKey();
        if ("application".equals(key)) {
          if (values.size() > 1) {
            respondBadRequest(message, request, helper, "more than one 'application' parameter");
            return;
          }
          application = value;
          continue;
        }
        filters.put(key, value);
      }

      @SuppressWarnings("unchecked")
      List<MetaDataEntry> entries = Collections.EMPTY_LIST;
      try {
        if (entryId == null) {
          // no entryId given -> list
          entries = this.accessor.getMetaDataStore().list(opContext, accountId, application, type, filters);
        } else {
          if (!filters.isEmpty()) {
            respondBadRequest(message, request, helper, "filters are not allowed for a single entry id");
            return;
          }
          MetaDataEntry entry = this.accessor.getMetaDataStore().get(opContext, accountId, application, type, entryId);
          if (entry != null) {
            entries = Collections.singletonList(entry);
          }
        }
      } catch (OperationException e) {
        LOG.error("Error reading meta data for URL " + uri + ": " + e.getMessage(), e);
        respondError(message.getChannel(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
        helper.finish(Error);
        return;
      }

      // respond with 404 if nothing was found
      if (entries.isEmpty()) {
        helper.finish(NoData);
        respondError(message.getChannel(), HttpResponseStatus.NOT_FOUND);
        return;
      }
      // now convert the list of meta data entries to JSON
      byte[] response = gson.toJson(entries).getBytes(Charsets.UTF_8);
      respondSuccess(message.getChannel(), request, response);
      helper.finish(Success);

    } catch (Exception e) {
      LOG.error("Exception caught for connector '" +
                  this.accessor.getName() + "'. ", e);
      helper.finish(Error);
      if (message.getChannel().isOpen()) {
        respondError(message.getChannel(),
                     HttpResponseStatus.INTERNAL_SERVER_ERROR);
        message.getChannel().close();
      }
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
    throws Exception {
    MetricsHelper.meterError(metrics, this.accessor.getMetricsQualifier());
    LOG.error("Exception caught for connector '" +
                this.accessor.getName() + "'. ", e.getCause());
    if (e.getChannel().isOpen()) {
      respondError(e.getChannel(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
      e.getChannel().close();
    }
  }
}

package com.continuuity.gateway.accessor;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.continuuity.api.data.QueryRestProvider;
import com.continuuity.common.metrics.CMetrics;
import com.continuuity.gateway.Constants;
import com.continuuity.gateway.util.NettyRestHandler;

/**
 * This is the http request handler for the data lib rest accessor.
 * <p>
 * At this time it only accepts GET requests to retrieve a value for a key from a named table.
 * 
 * http://host:port&lt;pathPrefix>&lt;querytype>/&lt;querymethod>?key=value
 * For instance, if config(prefix="/v0.1/" path="feedreader/"),
 * then pathPrefix will be "/v0.1/feedreader/", and a valid request is
 * GET http://host:port/v0.1/feedreader/getfeed?userid=100
 */
public class QueryRestHandler extends NettyRestHandler {

  private static final Logger LOG = LoggerFactory
      .getLogger(QueryRestHandler.class);

  /**
   * The allowed methods for this handler
   */
  HttpMethod[] allowedMethods = {
      HttpMethod.GET
  };

  /**
   * Will help validate URL paths, and also has the name of the connector and
   * the data fabric executor.
   */
  private QueryRestAccessor accessor;

  /**
   * The metrics object of the rest accessor
   */
  private CMetrics metrics;

  /**
   * The query rest provider which actually implements the processing of the
   * query request.
   */
  private QueryRestProvider provider;

  /**
   * Disallow default constructor
   */
  @SuppressWarnings("unused")
  private QueryRestHandler() {  }

  /**
   * Constructor requires the accessor that created this
   *
   * @param accessor the accessor that created this
   */
  QueryRestHandler(QueryRestAccessor accessor, QueryRestProvider provider) {
    this.accessor = accessor;
    this.metrics = accessor.getMetricsClient();
    this.provider = provider;
  }

  @Override
  public void messageReceived(ChannelHandlerContext context,
                              MessageEvent message) throws Exception {
    this.provider.executeQuery(message);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
      throws Exception {
    metrics.meter(this.getClass(), Constants.METRIC_INTERNAL_ERRORS, 1);
    LOG.error("Exception caught for connector '" +
        this.accessor.getName() + "'. ", e.getCause());
    e.getChannel().close();
  }
}

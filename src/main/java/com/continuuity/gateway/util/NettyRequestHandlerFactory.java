package com.continuuity.gateway.util;

import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

/**
 * This class is used by @ref NettyHttpPipelineFactory to create the request
 * handler for the pipeline. Note that all of our Http pipelines have a common
 * pipeline, implementing the Http protocol, with the main difference being
 * their actual request handler (which has all the business logic).
 */
public interface NettyRequestHandlerFactory {
  /**
   * The only method in this class creates a new instance of the request handler.
   *
   * @return the new request handler
   */
  SimpleChannelUpstreamHandler newHandler();
}

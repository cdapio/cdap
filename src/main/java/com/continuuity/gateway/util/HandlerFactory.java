package com.continuuity.gateway.util;

import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

public interface HandlerFactory {

	SimpleChannelUpstreamHandler newHandler();
}

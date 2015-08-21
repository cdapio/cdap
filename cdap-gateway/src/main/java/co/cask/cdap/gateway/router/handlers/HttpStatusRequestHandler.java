/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.gateway.router.handlers;

import co.cask.cdap.common.conf.Constants;
import com.google.common.base.Charsets;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;

/**
 * Handler for the status requests
 */
public class HttpStatusRequestHandler extends SimpleChannelUpstreamHandler {

  @Override
  public void messageReceived(ChannelHandlerContext ctx,
                              MessageEvent event) throws Exception {

    Object msg = event.getMessage();

    if (msg instanceof HttpRequest) {
        HttpRequest request = (HttpRequest) msg;
        // To have an independent health check of the router status, the command should
        // be served by the router itself without it talking to any downstream services
        if (request.getUri().equals(Constants.EndPoints.STATUS)) {
          String statusString = Constants.Monitor.STATUS_OK;
          ChannelBuffer responseContent = ChannelBuffers.wrappedBuffer(Charsets.UTF_8.encode(statusString));
          HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
          httpResponse.setHeader(HttpHeaders.Names.CONTENT_TYPE, "text/plain");
          httpResponse.setHeader(HttpHeaders.Names.CONTENT_LENGTH, responseContent.readableBytes());
          httpResponse.setContent(responseContent);

          ChannelFuture writeFuture = Channels.future(event.getChannel());
          Channels.write(ctx, writeFuture, httpResponse);
          writeFuture.addListener(ChannelFutureListener.CLOSE);
          return;
        }
    }
    super.messageReceived(ctx, event);
  }
}

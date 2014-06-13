package com.continuuity.internal.app.runtime.procedure;

import com.continuuity.api.procedure.ProcedureRequest;
import com.continuuity.common.metrics.MetricsCollector;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * This class dispatch HTTP requests to HandlerMethod. It uses thread local to control
 * how many instances of HandlerMethod created, hence it is supposed to be used shared
 * around all ChannelPipeline.
 */
final class ProcedureDispatcher extends SimpleChannelHandler {

  private static final Logger LOG = LoggerFactory.getLogger(ProcedureDispatcher.class);
  private static final Type REQUEST_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final Pattern REQUEST_URI_PATTERN = Pattern.compile("apps/(.+)/procedures/(.+)/methods/(.+)$");
  private static final Pattern METHOD_GET_PATTERN = Pattern.compile("^(.*?)[?]");
  private static final Gson GSON = new Gson();
  private static final Type QUERY_PARAMS_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final Maps.EntryTransformer<String, List<String>, String> MULTIMAP_TO_MAP_FUNCTION =
    new Maps.EntryTransformer<String, List<String>, String>() {
      @Override
      public String transformEntry(@Nullable String key, @Nullable List<String> value) {
        if (value == null || value.isEmpty()) {
          return null;
        }
        return value.get(0);
      }
    };

  private final MetricsCollector metrics;
  private final ThreadLocal<HandlerMethod> handlerMethod;

  ProcedureDispatcher(final HandlerMethodFactory handlerMethodFactory, MetricsCollector metrics) {
    this.metrics = metrics;
    handlerMethod = new ThreadLocal<HandlerMethod>() {
      @Override
      protected HandlerMethod initialValue() {
        return handlerMethodFactory.create();
      }
    };
  }

  @Override
  public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
    Object message = e.getMessage();
    if (!(message instanceof HttpRequest)) {
      super.messageReceived(ctx, e);
      return;
    }

    handleRequest((HttpRequest) message, ctx.getChannel());
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
    LOG.error("Exception caught in channel processing.", e.getCause());
    ctx.getChannel().close();
  }

  /**
   * Sends a error response and close the channel.
   * @param status Status of the response.
   * @param channel Netty channel for output.
   */
  private void errorResponse(HttpResponseStatus status, Channel channel, String content) {
    metrics.gauge("query.failed", 1);
    HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, status);
    response.setHeader(HttpHeaders.Names.CONTENT_TYPE, "text/plain; charset=utf-8");
    response.setContent(ChannelBuffers.wrappedBuffer(Charsets.UTF_8.encode(content)));
    Channels.write(channel, response).addListener(ChannelFutureListener.CLOSE);
  }

  private void handleRequest(HttpRequest httpRequest, Channel channel) {
    if (!(HttpMethod.POST.equals(httpRequest.getMethod()) || (HttpMethod.GET.equals(httpRequest.getMethod())))) {
      errorResponse(HttpResponseStatus.METHOD_NOT_ALLOWED, channel, "Only GET and POST methods are supported.");
      return;
    }

    Matcher uriMatcher = REQUEST_URI_PATTERN.matcher(httpRequest.getUri());
    if (!uriMatcher.find()) {
      errorResponse(HttpResponseStatus.BAD_REQUEST, channel, "Invalid request uri.");
      return;
    }

    String requestMethod = uriMatcher.group(3);

    ProcedureRequest request = createProcedureRequest(httpRequest, channel, requestMethod);
    if (request == null) {
      return;
    }

    // Lookup the request handler and handle
    HandlerMethod handler;
    try {
      handler = handlerMethod.get();
    } catch (Throwable t) {
      LOG.error("Fail to get procedure.", t);
      errorResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR, channel, "Fail to get procedure.");
      return;
    }
    handler.handle(request, new HttpProcedureResponder(channel));
  }

  private ProcedureRequest createProcedureRequest(HttpRequest request, Channel channel, String requestMethod) {
    try {
      Map<String, String> args;
      ChannelBuffer content;
      if (HttpMethod.POST.equals(request.getMethod())) {
        content = request.getContent();
      } else {
        //GET method - Get key/value pairs from the URI
        Map<String, List<String>> queryParams = new QueryStringDecoder(request.getUri()).getParameters();
        content = ChannelBuffers.EMPTY_BUFFER;

        if (!queryParams.isEmpty()) {
          content = jsonEncode(Maps.transformEntries(queryParams, MULTIMAP_TO_MAP_FUNCTION), QUERY_PARAMS_TYPE,
                               ChannelBuffers.dynamicBuffer(request.getUri().length()));
        }
      }

      if (content == null || !content.readable()) {
        args = ImmutableMap.of();
      } else {
        args = GSON.fromJson(new InputStreamReader(new ChannelBufferInputStream(content), Charsets.UTF_8),
                             REQUEST_TYPE);
      }

      //Extract the GET method name
      Matcher methodMatcher = METHOD_GET_PATTERN.matcher(requestMethod);
      if (methodMatcher.find()) {
        requestMethod = methodMatcher.group(1);
      }

      return new DefaultProcedureRequest(requestMethod, args);

    } catch (Exception ex) {
      errorResponse(HttpResponseStatus.BAD_REQUEST, channel, "Only json map<string,string> is supported.");
    }
    return null;
  }

  private <T> ChannelBuffer jsonEncode(T obj, Type type, ChannelBuffer buffer) throws IOException {
    Writer writer = new OutputStreamWriter(new ChannelBufferOutputStream(buffer), Charsets.UTF_8);
    try {
      GSON.toJson(obj, type, writer);
    } finally {
      writer.close();
    }
    return buffer;
  }

}

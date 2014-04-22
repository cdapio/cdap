package com.continuuity.logging.gateway.handlers;

import com.continuuity.http.HttpResponder;
import com.continuuity.logging.read.Callback;
import com.continuuity.logging.read.LogEvent;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.PatternLayout;
import com.google.common.collect.ImmutableMultimap;
import org.apache.commons.lang.StringEscapeUtils;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;

/**
 * LogReader callback to encode log events, and send them as chunked stream.
 */
class ChunkedLogReaderCallback implements Callback {
  private final ByteBuffer chunkBuffer = ByteBuffer.allocate(8 * 1024);
  private final CharsetEncoder charsetEncoder = Charset.forName("UTF-8").newEncoder();
  private final HttpResponder responder;
  private final PatternLayout patternLayout;
  private final boolean escape;

  ChunkedLogReaderCallback(HttpResponder responder, String logPattern, boolean escape) {
    this.responder = responder;
    this.escape = escape;

    ch.qos.logback.classic.Logger rootLogger =
      (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    LoggerContext loggerContext = rootLogger.getLoggerContext();

    this.patternLayout = new PatternLayout();
    this.patternLayout.setContext(loggerContext);
    this.patternLayout.setPattern(logPattern);
  }

  @Override
  public void init() {
    this.patternLayout.start();
    responder.sendChunkStart(HttpResponseStatus.OK,
                             ImmutableMultimap.of(HttpHeaders.Names.CONTENT_TYPE, "text/plain; charset=utf-8"));
  }

  @Override
  public void handle(LogEvent event) {
    String logLine = patternLayout.doLayout(event.getLoggingEvent());
    logLine = escape ? StringEscapeUtils.escapeHtml(logLine) : logLine;
    // Encode logLine and send chunks
    encodeSend(CharBuffer.wrap(logLine), false);
  }

  @Override
  public void close() {
    try {
      // Write the last chunk
      encodeSend(CharBuffer.allocate(0), true);
      // Flush the encoder
      CoderResult coderResult;
      do {
        coderResult = charsetEncoder.flush(chunkBuffer);
        chunkBuffer.flip();
        responder.sendChunk(ChannelBuffers.copiedBuffer(chunkBuffer));
        chunkBuffer.clear();
      } while (coderResult.isOverflow());

    } finally {
      try {
        patternLayout.stop();
      } finally {
        responder.sendChunkEnd();
      }
    }
  }

  private void encodeSend(CharBuffer inBuffer, boolean endOfInput) {
    while (true) {
      CoderResult coderResult = charsetEncoder.encode(inBuffer, chunkBuffer, endOfInput);
      if (coderResult.isOverflow()) {
        // if reached buffer capacity then flush chunk
        chunkBuffer.flip();
        responder.sendChunk(ChannelBuffers.copiedBuffer(chunkBuffer));
        chunkBuffer.clear();
      } else if (coderResult.isError()) {
        // skip characters causing error, and retry
        inBuffer.position(inBuffer.position() + coderResult.length());
      } else {
        // log line was completely written
        break;
      }
    }
  }
}

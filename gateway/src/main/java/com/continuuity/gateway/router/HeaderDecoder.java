package com.continuuity.gateway.router;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.handler.codec.http.HttpConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decode header from HTTP message without decoding the whole message.
 */
public class HeaderDecoder {
  private static final Logger LOG = LoggerFactory.getLogger(HeaderDecoder.class);
  private static final int MAX_HEADER_VALUE_LENGTH = 4096;

  public static String decodeHeader(ChannelBuffer buffer, String name) {
    // TODO: Add max HTTP message lenght check.
    name = name.toLowerCase();
    LOG.trace("Decoding header {}", name);

    int lineStart = buffer.readerIndex();
    char prevChar = (char) -1;
    int maxInd = name.length();
    boolean headerFound = false;

    // TODO: ignore initial white space and control characters

    // Find LF followed by the header name, or an empty line to end headers
    loop:
    while (buffer.readable()) {
      char currentChar = (char) buffer.readByte();

      if (currentChar == HttpConstants.LF) {
        // Found end of line
        if (prevChar == HttpConstants.CR && (buffer.readerIndex() - lineStart - 2) == 0) {
          // Empty line, end of headers
          LOG.trace("Found empty CRLF, end of headers");
          break;
        }

        lineStart = buffer.readerIndex();
        // Check if header name appears
        //noinspection LoopStatementThatDoesntLoop
        while (true) {
          int ind = 0;
          while (ind < maxInd && buffer.readable() &&
            name.charAt(ind++) == (currentChar = Character.toLowerCase((char) buffer.readByte()))) {
            // empty body
          }

          if (ind == maxInd) {
            // Found the header
            headerFound = true;
            LOG.trace("Found header name {}", name);
            break loop;
          } else {
            break;
          }
        }
      }

      prevChar = currentChar;
    }

    if (headerFound) {
      // Now find header value

      // Skip ':'
      buffer.readByte();

      // Read header value
      StringBuilder stringBuilder = new StringBuilder();
      while (buffer.readable()) {
        char currentChar = (char) buffer.readByte();

        if (currentChar == HttpConstants.LF) {
          // Got LF
          // TODO: add support for multi line header
          break;
        }
        stringBuilder.append(currentChar);
        if (stringBuilder.length() > MAX_HEADER_VALUE_LENGTH) {
          LOG.warn("Max header value reached for header {}: {}. Ignoring it.",
                   name, stringBuilder.toString());
          return null;
        }
      }
      String value = stringBuilder.toString().trim();
      LOG.trace("Found header {}:{}", name, value);
      return value;
    }

    // Header not found, return null
    LOG.trace("Header {} not found", name);
    return null;
  }
}

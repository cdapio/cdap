package com.continuuity.gateway.router;

import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.handler.codec.http.HttpConstants;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

/**
 * Decode header from HTTP message without decoding the whole message.
 */
public class HeaderDecoder {
  private static final Logger LOG = LoggerFactory.getLogger(HeaderDecoder.class);
  private static final int MAX_HEADER_VALUE_LENGTH = 4096;

  private static final String HOST_HEADER_STR = "\r\n" + HttpHeaders.Names.HOST.toLowerCase() + ":";
  private static final int HOST_HEADER_STR_MAX_IND = HOST_HEADER_STR.length() - 1;

  public static HeaderInfo decodeHeader(ChannelBuffer buffer) {
    try {
      int endIndex = Math.min(buffer.readableBytes(), MAX_HEADER_VALUE_LENGTH);

      // Find the first space.
      int firstSpace = buffer.indexOf(buffer.readerIndex(), endIndex, HttpConstants.SP);
      if (firstSpace == -1) {
        LOG.trace("No first space found");
        return null;
      }

      int secondSpace = buffer.indexOf(firstSpace + 1, endIndex, HttpConstants.SP);
      if (secondSpace == -1) {
        LOG.trace("No second space found");
        return null;
      }

      String path =  buffer.slice(firstSpace + 1, secondSpace - firstSpace - 1).toString(Charsets.UTF_8);
      if (path.contains("://")) {
        path = new URI(path).getRawPath();
      }

      // Find Host header.
      int fromIndex = secondSpace + 1;
      int hostInd = 0;
      while (fromIndex < endIndex) {
        if (Character.toLowerCase((char) buffer.getByte(fromIndex++)) != HOST_HEADER_STR.charAt(hostInd++)) {
          hostInd = 0;
        }

        if (hostInd == HOST_HEADER_STR_MAX_IND) {
          break;
        }
      }

      // Host header not found.
      if (fromIndex == endIndex) {
        LOG.trace("Host header not found");
        return null;
      }

      // Read host header
      ++fromIndex;
      int crIndex = buffer.indexOf(fromIndex, endIndex, HttpConstants.CR);
      if (crIndex == -1) {
        LOG.info("cr at end of host header not found");
        return null;
      }

      String host = buffer.slice(fromIndex, crIndex - fromIndex).toString(Charsets.UTF_8);
      HeaderInfo headerInfo = new HeaderInfo(path, host.trim());
      LOG.trace("Returning header info {}", headerInfo);
      return headerInfo;

    } catch (Throwable e) {
      LOG.debug("Got exception while decoding header: ", e);
      return null;
    }
  }

  /**
   * Decoded header information.
   */
  public static class HeaderInfo {
    private final String path;
    private final String host;

    public HeaderInfo(String path, String host) {
      this.path = path;
      this.host = host;
    }

    public String getPath() {
      return path;
    }

    public String getHost() {
      return host;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("path", path)
        .add("host", host)
        .toString();
    }
  }
}

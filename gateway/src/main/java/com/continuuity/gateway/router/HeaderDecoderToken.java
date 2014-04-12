package com.continuuity.gateway.router;
import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.HttpConstants;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

/**
 *
 */
public class HeaderDecoderToken{
  private static final Logger LOG = LoggerFactory.getLogger(HeaderDecoder.class);
  private static final int MAX_HEADER_VALUE_LENGTH = 4096;

  private static final String HOST_HEADER_STR = "\r\n" + HttpHeaders.Names.HOST.toLowerCase() + ":";
  private static final String AUTH_HEADER_STR = "\r\n" + "Authorization: Bearer";
  private static final int HOST_HEADER_STR_MAX_IND = HOST_HEADER_STR.length() - 1;

  public static HeaderInfo decodeHeader(ChannelBuffer buffer) {
    try {
        int endIndex = Math.min(buffer.readableBytes(), MAX_HEADER_VALUE_LENGTH);

        // Find the first space.
        int firstSpace = buffer.indexOf(buffer.readerIndex(), endIndex, HttpConstants.SP);
        if (firstSpace == -1) {
          LOG.debug("No first space found");
          return null;
        }

        int secondSpace = buffer.indexOf(firstSpace + 1, endIndex, HttpConstants.SP);
        if (secondSpace == -1) {
          LOG.debug("No second space found");
          return null;
        }

        String path =  buffer.slice(firstSpace + 1, secondSpace - firstSpace - 1).toString(Charsets.UTF_8);
        if (path.contains("://")) {
          path = new URI(path).getRawPath();
        }

        // Find Host header.
        int fromIndex = secondSpace + 1;
        int hostInd = 0;
        String tmpBuffer = "";

        boolean hostFound = false;
        boolean tokenFound = false;
        HeaderInfo headerInfo = null;

        while (fromIndex < endIndex) {
          tmpBuffer = tmpBuffer + Character.toLowerCase((char) buffer.getByte(fromIndex++));
          if (tmpBuffer.endsWith(HOST_HEADER_STR)){
            hostFound = true;
            break;
          }
          else if (tmpBuffer.endsWith(AUTH_HEADER_STR)){
            tokenFound = true;
            break;
          }
        }

        // Host header not found.
        if (fromIndex == endIndex) {
          LOG.debug("Host header not found");
          return null;
        }

        if (hostFound){
          // Read host header
          ++fromIndex;
          int crIndex = buffer.indexOf(fromIndex, endIndex, HttpConstants.CR);
          fromIndex = crIndex;
          if (crIndex == -1) {
            LOG.info("cr at end of host header not found");
            return null;
          }
          String host = buffer.slice(fromIndex, crIndex - fromIndex).toString(Charsets.UTF_8);
          headerInfo = new HeaderInfo(path, host.trim());
        }



        LOG.trace("Returning header info {}", headerInfo);
        return headerInfo;

    } catch (Throwable e) {
      LOG.error("Got exception while decoding header: ", e);
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

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      HeaderInfo that = (HeaderInfo) o;

      return !(host != null ? !host.equals(that.host) : that.host != null) &&
        !(path != null ? !path.equals(that.path) : that.path != null);

    }

    @Override
    public int hashCode() {
      int result = path != null ? path.hashCode() : 0;
      result = 31 * result + (host != null ? host.hashCode() : 0);
      return result;
    }
  }

  public static void main(String[] args){
    String message =
      "GET http://www.continuuity.com:9876/v2/apps HTTP/1.1\r\n" +
        "Connection: close\r\n" +
        "User-Agent: Web-sniffer/1.0.46 (+http://web-sniffer.net/)\r\n" +
        "Accept-Encoding: gzip\r\n" +
        "Accept-Charset:     ISO-8859-1,UTF-8;q=0.7,*;q=0.7   \r\n" +
        "Cache-Control: no-cache\r\n" +
        "Accept-Language: de,en;q=0.7,en-us;q=0.3\r\n" +
        "Authorization:  testtoken123\r\n" +
        "Referer: http://web-sniffer.net/\r\n" +
        "Host:    www.continuuity.com   \r\n" +
        "\r\n" +
        "Message-body: message body\r\n";

    HeaderDecoder.HeaderInfo headerInfo =
      HeaderDecoder.decodeHeader(ChannelBuffers.wrappedBuffer(message.getBytes(Charsets.UTF_8)));
    System.out.println(headerInfo);
  }
}

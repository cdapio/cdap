package com.continuuity.gateway.router;

import com.continuuity.common.conf.Constants;
import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.handler.codec.http.HttpConstants;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Decode header from HTTP message without decoding the whole message.
 */
public class HeaderDecoder {
  private static final Logger LOG = LoggerFactory.getLogger(HeaderDecoder.class);
  private static final int MAX_HEADER_VALUE_LENGTH = 4096;

  private static final String HOST_HEADER_STR = "\r\n" + HttpHeaders.Names.HOST.toLowerCase() + ":";
  private static final int HOST_HEADER_STR_MAX_IND = HOST_HEADER_STR.length() - 1;
  private static final Pattern API_KEY_PRECURSOR = Pattern.compile(Constants.Gateway.CONTINUUITY_API_KEY);

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

      int thirdSpace = buffer.indexOf(secondSpace + 1, endIndex, HttpConstants.SP);
      if (thirdSpace == -1) {
        LOG.debug("No third space found");
        return null;
      }

      int firstCR = buffer.indexOf(buffer.readerIndex(), endIndex, HttpConstants.CR);
      if (firstCR == -1) {
        LOG.debug("No first carriage return found");
        return null;
      }

      int secondCR = buffer.indexOf(firstCR + 1, endIndex, HttpConstants.CR);
      if (secondCR == -1) {
        LOG.debug("No second carriage return found");
        return null;
      }

      byte colon = 58;
      int firstColon = buffer.indexOf(firstCR + 1, endIndex, colon);
      if (firstColon == -1) {
        LOG.debug("No first colon found");
        return null;
      }

      String version = buffer.slice(secondSpace + 1, firstCR - secondSpace - 1).toString(Charsets.UTF_8);
      String method = buffer.slice(0, firstSpace).toString(Charsets.UTF_8);
      String path =  buffer.slice(firstSpace + 1, secondSpace - firstSpace - 1).toString(Charsets.UTF_8);
      String apiPre = buffer.slice(firstCR + 1, firstColon - firstCR - 1).toString(Charsets.UTF_8);
      String apiKey = "";
      Matcher matchAPIPre = API_KEY_PRECURSOR.matcher(apiPre);
      if (matchAPIPre.find()) {
        apiKey = buffer.slice(thirdSpace + 1, secondCR - thirdSpace - 1).toString(Charsets.UTF_8);
      }

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
        LOG.debug("Host header not found");
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
      HeaderInfo headerInfo = new HeaderInfo(path, host.trim(), method.trim(), apiKey.trim(), version.trim());
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
    private final String method;
    private final String apiKey;
    private final String version;

    public HeaderInfo(String path, String host, String method, String apiKey, String version) {
      this.path = path;
      this.host = host;
      this.method = method;
      this.apiKey = apiKey;
      this.version = version;
    }

    public String getPath() {
      return path;
    }

    public String getHost() {
      return host;
    }

    public String getMethod() { return method; }

    public String getAPIKey() { return apiKey; }

    public String getVersion() { return version; }

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
}

package com.continuuity.gateway.router;


import com.google.common.base.Objects;
import org.jboss.netty.buffer.ChannelBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.net.URI;

/**
 * Decode header from HTTP message without decoding the whole message.
 */
public class HeaderDecoder {
  private static final Logger LOG = LoggerFactory.getLogger(HeaderDecoder.class);

  public static HeaderInfo decodeHeader(ChannelBuffer buffer){
    try {
      HeaderInfo headerInfo = null;

      byte[] msgBytes = new byte[buffer.readableBytes()];
      buffer.getBytes(buffer.readerIndex(), msgBytes);
      String msg = new String(msgBytes);
      Scanner sc = new Scanner(msg);
      Map<String, String> httpFieldMap = new HashMap<String, String>();
      while (sc.hasNext()){
        String line = sc.nextLine();
        String []fragments = line.split("\\s+");
        if (fragments.length > 0){
          if (fragments[0].equals("GET") || fragments[0].equals("POST") && fragments[1] != null){
            String path;
            path = fragments[1];
            if (fragments[1].indexOf("http://") != -1) {
              path = new URI(fragments[1]).getRawPath();
            }
            httpFieldMap.put("path", path);
          }
          else if (fragments[0].toLowerCase().equals("Host:".toLowerCase()) && fragments[1] != null){
            httpFieldMap.put("host", fragments[1]); }
          else if (fragments.length > 2 && fragments[0].toLowerCase().equals("Authorization:".toLowerCase())
            && fragments[1].equals("Bearer")){
            httpFieldMap.put("token", fragments[2].trim());
          }
        }
      }
      if (!httpFieldMap.containsKey("path") || !httpFieldMap.containsKey("host")){
        return null;
      }
      headerInfo = new HeaderInfo(httpFieldMap.get("path"), httpFieldMap.get("host"), httpFieldMap.get("token"));
      return headerInfo;
    }
    catch (Throwable e){
      LOG.error("Got exception while decoding header");
      return null;
    }
  }




  /**
   * Decoded header information.
   */
  public static class HeaderInfo {
    private final String path;
    private final String host;
    private final String token;

    public HeaderInfo(String path, String host) {
      this.path = path;
      this.host = host;
      this.token = null;
    }
    public HeaderInfo(String path, String host, String token){
      this.path = path;
      this.host = host;
      this.token = token;
    }
    public String getPath() {
      return path;
    }

    public String getHost() {
      return host;
    }

    public String getToken() { return token; }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("path", path)
        .add("host", host)
        .add("token", token)
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

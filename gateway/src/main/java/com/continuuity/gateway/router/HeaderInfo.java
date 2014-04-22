package com.continuuity.gateway.router;


import com.google.common.base.Objects;

/**
 * Decoded header information.
 */
public class HeaderInfo {
  private final String path;
  private final String host;
  private final String method;

  public HeaderInfo(String path, String host, String method) {
    this.path = path;
    this.host = host;
    this.method = method;
  }

  public String getPath() {
    return path;
  }

  public String getHost() {
    return host;
  }

  public String getMethod() {
    return method;
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
    return Objects.equal(host, that.host) && Objects.equal(path, that.path);

  }

  @Override
  public int hashCode() {
    int result = path != null ? path.hashCode() : 0;
    result = 31 * result + (host != null ? host.hashCode() : 0);
    return result;
  }
}

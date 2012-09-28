package com.continuuity.log.common;

import org.apache.commons.codec.CharEncoding;
import org.apache.commons.io.IOUtils;
import org.mortbay.jetty.HttpHeaders;

import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.TimeUnit;

/**
 * Abstract feeder through HTTP POST request.
 * <p>The class has to be public in order to allow LOG4J or Logback to access it.
 * <p>The class is thread-safe.
 */
public abstract class AbstractHttpFeeder implements Feeder {

  /**
   * The URL to post to.
   */
  private transient URL url;

  /**
   * Set option {@code url}.
   * @param addr The URL
   * @throws java.net.MalformedURLException If format of the URL is
   *  not correct
   */
  public final void setUrl(final String addr)
    throws java.net.MalformedURLException {
    if(addr == null) {
      throw new NullPointerException("Address cannot be null.");
    }
    this.url = new URL(addr);
  }

  /**
   * Get URL.
   * @return The URL
   */
  protected final URL getUrl() {
    if (this.url == null) {
      throw new IllegalStateException("URL is not configured");
    }
    return this.url;
  }

  /**
   * POST one line of text.
   * @param text The text to post
   */
  protected final void post(final String text) throws IOException {
    final HttpURLConnection conn =
      (HttpURLConnection) this.getUrl().openConnection();
    try {
      conn.setConnectTimeout((int) TimeUnit.MINUTES.toMillis(1L));
      conn.setReadTimeout((int) TimeUnit.MINUTES.toMillis(1L));
      conn.setDoOutput(true);
      try {
        conn.setRequestMethod("POST");
      } catch (java.net.ProtocolException ex) {
        throw new IOException(ex);
      }
      conn.setRequestProperty(
        HttpHeaders.CONTENT_TYPE,
        MediaType.TEXT_PLAIN
      );
      IOUtils.write(text, conn.getOutputStream(), CharEncoding.UTF_8);
      if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
        throw new IOException(
          String.format("Invalid response code #%d from %s",
                        conn.getResponseCode(), this.url)
        );
      }
    } finally {
      conn.disconnect();
    }
  }

}
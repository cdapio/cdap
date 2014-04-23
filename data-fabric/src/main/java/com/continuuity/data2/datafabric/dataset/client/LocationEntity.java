package com.continuuity.data2.datafabric.dataset.client;

import org.apache.twill.filesystem.Location;
import com.google.common.base.Throwables;
import org.apache.http.entity.AbstractHttpEntity;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 *
 */
public class LocationEntity extends AbstractHttpEntity {

  private final Location location;

  public LocationEntity(final Location location, final String contentType) {
    super();
    if (location == null) {
      throw new IllegalArgumentException("Lcoation may not be null");
    }
    this.location = location;
    setContentType(contentType);
  }

  public boolean isRepeatable() {
    return true;
  }

  public long getContentLength() {
    try {
      return location.length();
    } catch (IOException e) {
      // todo
      throw Throwables.propagate(e);
    }
  }

  public InputStream getContent() throws IOException {
    return location.getInputStream();
  }

  public void writeTo(final OutputStream outputStream) throws IOException {
    if (outputStream == null) {
      throw new IllegalArgumentException("Output stream may not be null");
    }
    InputStream inputStream = location.getInputStream();
    try {
      byte[] buff = new byte[4096];
      int l;
      while ((l = inputStream.read(buff)) != -1) {
        outputStream.write(buff, 0, l);
      }
      outputStream.flush();
    } finally {
      inputStream.close();
    }
  }

  /**
   * Tells that this entity is not streaming.
   *
   * @return <code>false</code>
   */
  public boolean isStreaming() {
    return false;
  }
}

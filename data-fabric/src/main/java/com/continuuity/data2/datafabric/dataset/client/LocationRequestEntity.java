package com.continuuity.data2.datafabric.dataset.client;

import org.apache.twill.filesystem.Location;
import com.google.common.base.Throwables;
import org.apache.commons.httpclient.methods.RequestEntity;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 *
 */
// todo: do we need it?
public class LocationRequestEntity implements RequestEntity {

  final Location location;
  final String contentType;

  public LocationRequestEntity(final Location location, final String contentType) {
    super();
    if (location == null) {
      throw new IllegalArgumentException("Location may not be null");
    }
    this.location = location;
    this.contentType = contentType;
  }

  public long getContentLength() {
    // todo: weird try/catch?
    try {
      return location.length();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  public String getContentType() {
    return contentType;
  }

  public boolean isRepeatable() {
    return true;
  }

  public void writeRequest(final OutputStream out) throws IOException {
    // todo: bigger buffer?
    byte[] tmp = new byte[4096];
    InputStream inStream = location.getInputStream();
    try {
      int i;
      while ((i = inStream.read(tmp)) >= 0) {
        out.write(tmp, 0, i);
      }
    } finally {
      inStream.close();
    }
  }

}

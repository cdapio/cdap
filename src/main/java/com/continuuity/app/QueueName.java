package com.continuuity.app;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;

import java.io.File;
import java.net.URI;

/**
 *
 */
public final class QueueName {

  private final URI uri;
  private final String simpleName;

  public static QueueName from(URI uri) {
    Preconditions.checkNotNull(uri, "URI cannot be null.");
    return new QueueName(uri);
  }

  public static QueueName from(byte[] bytes) {
    return new QueueName(URI.create(new String(bytes, Charsets.US_ASCII)));
  }

  private QueueName(URI uri) {
    this.uri = uri;
    this.simpleName = new File(uri.getPath()).getName();
  }

  public String getSimpleName() {
    return simpleName;
  }

  public byte[] toBytes() {
    return toString().getBytes(Charsets.US_ASCII);
  }

  @Override
  public String toString() {
    return uri.toASCIIString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    return uri.equals(((QueueName)o).uri);
  }

  @Override
  public int hashCode() {
    return uri.hashCode();
  }
}

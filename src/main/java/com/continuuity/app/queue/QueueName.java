package com.continuuity.app.queue;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;

import java.io.File;
import java.net.URI;

/**
 * An abstraction over URI of a queue.
 */
public final class QueueName {

  /**
   * URI of the queue.
   */
  private final URI uri;

  /**
   * End point name.
   */
  private final String simpleName;

  /**
   * Constructs this class from an URI.
   *
   * @param uri of the queue
   * @return An instance of {@link QueueName}
   */
  public static QueueName from(URI uri) {
    Preconditions.checkNotNull(uri, "URI cannot be null.");
    return new QueueName(uri);
  }

  /**
   * Constructs this class from byte array of queue URI.
   *
   * @param bytes respresenting URI
   * @return An instance of {@link QueueName}
   */
  public static QueueName from(byte[] bytes) {
    return new QueueName(URI.create(new String(bytes, Charsets.US_ASCII)));
  }

  /**
   * Called from static method {@code QueueName#from(URI)} and {@code QueueName#from(bytes[])}
   *
   * @param uri of the queue.
   */
  private QueueName(URI uri) {
    this.uri = uri;
    this.simpleName = new File(uri.getPath()).getName();
  }

  /**
   * @return Simple name which is the last part of queue URI path and endpoint.
   */
  public String getSimpleName() {
    return simpleName;
  }

  /**
   * @return bytes representation of queue uri.
   */
  public byte[] toBytes() {
    return toString().getBytes(Charsets.US_ASCII);
  }

  /**
   * @return converts to ascii string.
   */
  @Override
  public String toString() {
    return uri.toASCIIString();
  }

  /**
   * Compares this {@link QueueName} with another {@link QueueName}.
   *
   * @param o Other instance of {@link QueueName}
   * @return true if equal; false otherwise.
   */
  @Override
  public boolean equals(Object o) {
    if(this == o) {
      return true;
    }
    if(o == null || getClass() != o.getClass()) {
      return false;
    }

    return uri.equals(( (QueueName) o ).uri);
  }

  /**
   * @return hash code of this QueueName.
   */
  @Override
  public int hashCode() {
    return uri.hashCode();
  }
}

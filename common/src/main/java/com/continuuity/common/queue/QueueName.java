package com.continuuity.common.queue;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
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
   * Represents the queue as byte[].
   */
  private final byte[] byteName;

  /**
   * Represents the queue name as a string.
   */
  private final String stringName;

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

  public static QueueName fromFlowlet(String flow, String flowlet, String output) {
    URI uri = URI.create(Joiner.on("/").join("queue:", "", flow, flowlet, output));
    return new QueueName(uri);
  }

  /**
   * Generates an QueueName for the stream.
   *
   * @param accountId The stream belongs to
   * @param stream  connected to flow
   * @return An {@link QueueName} with schema as stream
   */
  public static QueueName fromStream(String accountId, String stream) {
    URI uri = URI.create(Joiner.on("/").join("stream:", "", accountId, stream));
    return new QueueName(uri);
  }


  /**
   * Called from static method {@code QueueName#from(URI)} and {@code QueueName#from(bytes[])}
   *
   * @param uri of the queue.
   */
  private QueueName(URI uri) {
    this.uri = uri;
    this.simpleName = new File(uri.getPath()).getName();
    this.stringName = uri.toASCIIString();
    this.byteName = stringName.getBytes(Charsets.US_ASCII);
  }

  public boolean isStream() {
    return "stream".equals(uri.getScheme());
  }

  /**
   * @return Simple name which is the last part of queue URI path and endpoint.
   */
  public String getSimpleName() {
    return simpleName;
  }

  /**
   * Gets the bytes representation of the queue uri. Note that mutating the returned array will mutate the underlying
   * byte array as well. If mutation is needed, one has to copy to a separate array.
   *
   * @return bytes representation of queue uri.
   */
  public byte[] toBytes() {
    return byteName;
  }

  /**
   * @return A {@link URI} representation of the queue name.
   */
  public URI toURI() {
    return uri;
  }

  /**
   * @return converts to ascii string.
   */
  @Override
  public String toString() {
    return stringName;
  }

  /**
   * Compares this {@link QueueName} with another {@link QueueName}.
   *
   * @param o Other instance of {@link QueueName}
   * @return true if equal; false otherwise.
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    return uri.equals(((QueueName) o).uri);
  }

  /**
   * @return hash code of this QueueName.
   */
  @Override
  public int hashCode() {
    return uri.hashCode();
  }
}

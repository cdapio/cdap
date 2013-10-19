package com.continuuity.common.queue;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.io.File;
import java.net.URI;
import java.util.List;

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
   * The components of the URI.
   */
  private final String[] components;

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

  public static QueueName fromFlowlet(String app, String flow, String flowlet, String output) {
    URI uri = URI.create(Joiner.on("/").join("queue:", "", app, flow, flowlet, output));
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
   * Called from static method {@code QueueName#from(URI)} and {@code QueueName#from(bytes[])}.
   *
   * @param uri of the queue.
   */
  private QueueName(URI uri) {
    this.uri = uri;
    this.simpleName = new File(uri.getPath()).getName();
    this.stringName = uri.toASCIIString();
    this.byteName = stringName.getBytes(Charsets.US_ASCII);
    List<String> comps = Lists.asList(uri.getHost(), uri.getPath().split("/"));
    this.components = comps.toArray(new String[comps.size()]);
  }

  public boolean isStream() {
    return "stream".equals(uri.getScheme());
  }

  public boolean isQueue() {
    return "queue".equals(uri.getScheme());
  }

  private String getNthComponent(int n) {
    return n < components.length ? components[n] : null;
  }

  /**
   * @return the first component of the URI (the app for a queue, the account for a stream).
   */
  public String getFirstComponent() {
    return getNthComponent(0);
  }

  /**
   * @return the second component of the URI (the flow for a queue, the stream name for a stream).
   */
  public String getSecondComponent() {
    return getNthComponent(1);

  }

  /**
   * @return the third component of the URI (the flowlet for a queue, null for a stream).
   */
  public String getThirdComponent() {
    return getNthComponent(2);
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
   * @return whether the given string represents a queue.
   */
  public static boolean isQueue(String name) {
    return name.startsWith("queue");
  }

  /**
   * @return whether the given string represents a queue.
   */
  public static boolean isStream(String name) {
    return name.startsWith("stream");
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

/*
 * Copyright © 2014-2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.common.queue;

import co.cask.cdap.proto.Id;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

import java.net.URI;
import java.util.Iterator;

/**
 * An abstraction over URI of a queue.
 */
public final class QueueName {

  /**
   * URI of the queue.
   */
  private final URI uri;

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

  public static QueueName fromFlowlet(String namespace, String app, String flow, String flowlet, String output) {
    URI uri = URI.create(String.format("queue:///%s/%s/%s/%s/%s", namespace, app, flow, flowlet, output));
    return new QueueName(uri);
  }

  public static String prefixForFlow(String namespace, String app, String flow) {
    // queue:///namespace/app/flow/
    // Note that the trailing / is crucial, otherwise this could match queues of flow1, flowx, etc.
    return String.format("queue:///%s/%s/%s/", namespace, app, flow);
  }

  // Note that like above the trailing '/' in the prefix for namespace is crucial,
  // otherwise this could match namespaces of ns, ns1, nsx, etc.
  public static String prefixForNamespacedQueue(String namespace) {
    // queue:///namespace/
    return String.format("queue:///%s/", namespace);
  }

  public static String prefixForNamedspacedStream(String namespace) {
    // stream:///namespace/
    return String.format("stream:///%s/", namespace);
  }

  /**
   * Generates an QueueName for the stream.
   *
   * @param namespace of the stream
   * @param stream name of the stream
   * @return An {@link QueueName} with schema as stream
   */
  public static QueueName fromStream(String namespace, String stream) {
    // The old stream admin uses full URI of queue name as the name
    URI uri = URI.create(String.format("stream:///%s/%s", namespace, stream));
    return new QueueName(uri);
  }

  public static QueueName fromStream(Id.Stream streamId) {
    return fromStream(streamId.getNamespaceId(), streamId.getName());
  }

  public Id.Stream toStreamId() {
    return Id.Stream.from(getFirstComponent(), getSecondComponent());
  }

  /**
   * Called from static method {@code QueueName#from(URI)} and {@code QueueName#from(bytes[])}.
   *
   * @param uri of the queue.
   */
  private QueueName(URI uri) {
    this.uri = uri;
    this.stringName = uri.toASCIIString();
    this.byteName = stringName.getBytes(Charsets.US_ASCII);
    Iterable<String> comps = Splitter.on('/').omitEmptyStrings().split(uri.getPath());
    components = new String[Iterables.size(comps)];
    Iterator<String> iter = comps.iterator();
    for (int i = 0; i < components.length; i++) {
      components[i] = iter.next();
    }
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
   * @return the first component of the of the URI (the namespace)
   */
  public String getFirstComponent() {
    return getNthComponent(0);
  }

  /**
   * @return the second component of the URI (the app for a queue, the stream name for a stream).
   */
  public String getSecondComponent() {
    return getNthComponent(1);
  }

  /**
   * @return the third component of the URI (the flow for a queue, null for a stream).
   */
  public String getThirdComponent() {
    return getNthComponent(2);
  }

  /**
   * @return the fourth component of the URI (the flowlet for a queue, null for a stream).
   */
  public String getFourthComponent() {
    return getNthComponent(3);
  }

  /**
   * @return Simple name which is the last part of queue URI path and endpoint.
   */
  public String getSimpleName() {
    return components[components.length - 1];
  }

  /**
   * @return the number of components in the queue name
   */
  public int getNumComponents() {
    return components.length;
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

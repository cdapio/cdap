package com.continuuity.internal.app.services.legacy;


import com.continuuity.common.utils.ImmutablePair;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Collection;
import java.util.List;

/**
 *
 */
public final class StreamNamerImpl {
  private static final Logger LOG = LoggerFactory.getLogger(StreamNamerImpl.class);

  // we will keep record of the stream types and URIs that we have assigned in
  // a hash map. following are two helper classes for the keys and values in
  // that hash map.

  private static class StreamName {
    private StreamType type;
    private URI uri;

    public StreamName(StreamType type, URI uri) {
      if (type == null) {
        IllegalArgumentException e =
            new IllegalArgumentException("Parameter 'type' may not be null.");
        LOG.error(e.getMessage());
        throw e;
      }
      if (uri == null) {
        IllegalArgumentException e =
            new IllegalArgumentException("Parameter 'uri' may not be null.");
        LOG.error(e.getMessage());
        throw e;
      }
      this.type = type;
      this.uri = uri;
    }

    public StreamType getType() {
      return this.type;
    }

    public URI getUri() {
      return this.uri;
    }
  }

  private static class StreamKey {
    private String flowlet;
    private String stream;

    // if the flowlet name is null, then this is a flow stream
    public StreamKey(String flowlet, String stream) {
      if (stream == null) {
        IllegalArgumentException e =
            new IllegalArgumentException("Parameter 'stream' may not be null.");
        LOG.error(e.getMessage());
        throw e;
      }
      this.flowlet = flowlet;
      this.stream = stream;
    }

    public String getFlowlet() {
      return flowlet;
    }

    public String getStream() {
      return stream;
    }

    public boolean equals(Object other) {
      return other instanceof StreamKey && this.equals((StreamKey) other);
    }

    public boolean equals(StreamKey other) {
      // first check that the flowlet name is the same,
      // be prepared to see nulls (for flow streams)
      if (this.flowlet == null) {
        if (other.flowlet != null) {
          return false;
        }
      } else {
        if (!this.flowlet.equals(other.flowlet)) {
          return false;
        }
      }
      // stream and type can never be null
      return this.stream.equals(other.stream);
    }

    public int hashCode() {
      return Objects.hashCode(this.flowlet, this.stream);
    }
  }

  /**
   * Generates and add the queue name to each flowlet based on it's connections.
   * @param accountName the account that the flow is deployed to
   * @param definition the flow definition
   * @return true if successful; false otherwise.
   */
  public boolean name(String accountName, FlowDefinition definition) {
    List<ImmutablePair<StreamKey, StreamName>> assignedStreams = Lists.newArrayList();
    Collection<? extends ConnectionDefinition> connections = definition.getConnections();

    String name = definition.getMeta().getName();

    // we will need this to modify the flow definition (to set the stream URIs)
    FlowDefinitionModifier modifier = (FlowDefinitionModifier) definition;

    /**
     * We iterate through each connection and construct URI. We do not modify
     * the definition till we finish processing through all. This ensures
     * that we don't have definition that is half modified.
     */
    for (ConnectionDefinition connection : connections) {
      // get the source and destination of this connection
      FlowletStreamDefinition from = connection.getFrom();
      FlowletStreamDefinition to = connection.getTo();

      // three cases:
      // 1. flow stream -> flowlet,
      // 2. (flowlet -> flow stream, currently unsupported)
      // 3. flowlet -> flowlet
      // 4. (flow stream -> flow stream, is not supported and thus ignored )

      if (to.isFlowStream()) {
        // case 2. or 4. -> this is not supported: log and return error;
        LOG.error("Unsupported connection to flow stream '{}'.",
                  new Object[]{connection.getTo().getStream()});
        return false;
      } else {
        // case 1. or 3., destination is an input of a flowlet.
        StreamKey toKey = new StreamKey(to.getFlowlet(), to.getStream());
        // based on the source of the connection, determine URI for the stream
        URI streamUri;
        if (connection.getFrom().isFlowStream()) {
          // case 1. flow stream -> flowlet
          // to determine the URI for the flow stream, get its name and form a
          // URI from the account name and the stream name
          String streamName = connection.getFrom().getStream();
          streamUri = FlowStream.buildStreamURI(accountName, streamName);
        } else {
          // case 3. flowlet -> flowlet. Perhaps we have already built this?
          StreamKey fromKey =
              new StreamKey(from.getFlowlet(), from.getStream());
          // First verify the flowlet is defined
          if (!verifyFlowlet(definition, from.getFlowlet())) {
            return false;
          }
          // Build the stream URI from flow/flowlet/stream name
          streamUri = FlowletStream.
              defaultURI(name, from.getFlowlet(), from.getStream());
          // and remember this URI and type for that flowlet and stream
          StreamName fromName = new StreamName(StreamType.OUT, streamUri);
          assignedStreams.add(new ImmutablePair<StreamKey, StreamName>(fromKey, fromName));
        }
        // we already checked that this in stream was not assigned before
        // now we know the correct URI, we can just add it to the assignments
        StreamName toName = new StreamName(StreamType.IN, streamUri);
        assignedStreams.add(new ImmutablePair<StreamKey, StreamName>(toKey, toName));
      }
    }
    // Now add all the assignments to the flow definition
    for (ImmutablePair<StreamKey, StreamName> stream : assignedStreams) {
      StreamKey streamKey = stream.getFirst();
      StreamName streamName = stream.getSecond();
      modifier.setStreamURI(streamKey.getFlowlet(), streamKey.getStream(),
          streamName.getUri(), streamName.getType());
    }
    // all fine
      return true;
  }

  /**
   * verify that a flowlet is defined in a flow definition
   * @param definition The flow definition
   * @param flowletName The flowlet name to be verified
   * @return whether the flowlet exists in the flow
   */
  private boolean verifyFlowlet(FlowDefinition definition, String flowletName) {
    for (FlowletDefinition flowlet : definition.getFlowlets()) {
      if (flowletName.equals(flowlet.getName())) {
        return true;
      }
    }
    LOG.error("No flowlet with name '{}' is defined in flow.", new Object[]{flowletName});
    return false;
  }
}

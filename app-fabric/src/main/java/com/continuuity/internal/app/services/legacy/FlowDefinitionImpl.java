package com.continuuity.internal.app.services.legacy;

import com.continuuity.common.utils.ImmutablePair;
import com.google.common.collect.Maps;

import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Implements FlowDefinitionModifier and FlowDefinition interfaces.
 */
public class FlowDefinitionImpl implements FlowDefinitionModifier, FlowDefinition {
  /**
   * Defines the meta section of a flow.
   */
  private MetaDefinitionImpl meta;

  /**
   * Defines a collection of flowlet definitions.
   */
  private Set<String> datasets;

  /**
   * Defines a collection of flowlet definitions.
   */
  private List<FlowletDefinitionImpl> flowlets;

  /**
   * Defines a collection of connections interconnecting flowlets.
   */
  private List<ConnectionDefinitionImpl> connections;

  /**
   * Defines a collection of connections interconnecting flowlets.
   */
  private List<FlowStreamDefinitionImpl> flowStreams;

  /**
   * Defines the mapping of flowlet name to flowletStreams name and their URI and type of stream it is.
   */
  private Map<String, Map<String, ImmutablePair<URI, StreamType>>> flowletStreams = Maps.newHashMap();

  /**
   * Returns the {@link MetaDefinition} section of a flow definition.
   *
   * @return meta section of the flow.
   */
  @Override
  public MetaDefinition getMeta() {
    return meta;
  }

  @Override
  public Set<String> getDatasets() {
    return datasets;
  }

  public void setDatasets(Set<String> sets) {
    datasets = sets;
  }

  /**
   * Sets a new meta to a flow.
   *
   * @param meta to be associated with the flow.
   */
  public void setMeta(MetaDefinitionImpl meta) {
    this.meta = meta;
  }

  /**
   * Returns collection of flowlets from a flow definition.
   *
   * @return collection of flowlet.
   */
  @Override
  public List<? extends FlowletDefinition> getFlowlets() {
    return flowlets;
  }

  /**
   * Sets a new collection of flowlets.
   *
   * @param flowlets collection of flowlet definition.
   */
  public void setFlowlets(List<FlowletDefinitionImpl> flowlets) {
    this.flowlets = flowlets;
  }

  /**
   * Sets a new collection of flow streams
   *
   * @param streams the list of flow stream defintions
   */
  public void setFlowStreams(List<FlowStreamDefinitionImpl> streams) {
    this.flowStreams = streams;
  }


  @Override
  public Collection<? extends FlowStreamDefinition> getFlowStreams() {
    return flowStreams;
  }

  /**
   * Returns a collection of connections connecting the flowlets.
   *
   * @return collection of connections.
   */
  @Override
  public List<? extends ConnectionDefinition> getConnections() {
    return connections;
  }

  /**
   * List of flowletStreams and URI's associated with a flowlet.
   *
   *
   * @param flowlet Name of the flowlet
   * @return List of flowletStreams associated with the flowlet along with their URIs
   */
  @Override
  public Map<String, ImmutablePair<URI, StreamType>> getFlowletStreams(String flowlet) {
    if (flowletStreams.containsKey(flowlet)) {
      return Collections.unmodifiableMap(flowletStreams.get(flowlet));
    }
    return null;
  }

  /**
   * Set a new connection inter-connecting the flowlets.
   *
   * @param connections collection of flowlet interconnects.
   */
  public void setConnections(List<ConnectionDefinitionImpl> connections) {
    this.connections = connections;
  }

  /**
   * Sets stream URI
   *
   * @param flowlet name of the flowlet
   * @param stream  name of the stream
   * @param uri     URI associated with the stream.
   */
  @Override
  public void setStreamURI(String flowlet, String stream, URI uri, StreamType type) {
    if (flowletStreams.containsKey(flowlet)) {
      flowletStreams.get(flowlet).put(stream + "_" + type, new ImmutablePair<URI, StreamType>(uri, type));
    } else {
      Map<String, ImmutablePair<URI, StreamType>> maps = Maps.newHashMap();
      maps.put(stream + "_" + type, new ImmutablePair<URI, StreamType>(uri, type));
      flowletStreams.put(flowlet, maps);
    }
  }


}

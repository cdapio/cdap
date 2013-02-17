package com.continuuity.internal.app.services.legacy;

import com.continuuity.common.utils.ImmutablePair;

import java.net.URI;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * FlowDefinition is a read-only interface for reading the attributes of
 * a flow definition. This interface is generally passed around internally
 * in the system to ensure that the definition can not be mutated. In some
 * cases we would need to modify a flow and add more information, in that
 * case we use the FlowDefinitionModifier interface to do so.
 */
public interface FlowDefinition {
  /**
   * Returns the {@link MetaDefinition} section of a flow definition.
   *
   * @return meta section of the flow.
   */
  public MetaDefinition getMeta();

  /**
   * Returns a sets of data set names used by the flow
   *
   * @return set of data set names
   */
  public Set<String> getDatasets();

  /**
   * Returns collection of flow inputs from a flow definition
   *
   * @return collection of input streams.
   */
  public Collection<? extends FlowStreamDefinition> getFlowStreams();

  /**
   * Returns collection of flowlets from a flow definition.
   *
   * @return collection of flowlet.
   */
  public Collection<? extends FlowletDefinition> getFlowlets();

  /**
   * Returns a collection of connections connecting the flowlets.
   *
   * @return collection of connections.
   */
  public Collection<? extends ConnectionDefinition> getConnections();

  /**
   * List of streams and URI's associated with a flowlet.
   *
   *
   * @param flowlet Name of the flowlet
   * @return List of streams associated with the flowlet along with their URIs
   */
  public Map<String, ImmutablePair<URI, StreamType>> getFlowletStreams(String flowlet);
}

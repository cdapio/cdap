package com.continuuity.internal.app.services.legacy;

import java.net.URI;

/**
 * FlowDefinitionModifier interface is used for modifying the definition.
 * There are cases were there is a need to modify the definition to annotate
 * FlowDefinition with more information. This is used only by internal systems.
 */
public interface FlowDefinitionModifier  {
  /**
   * Sets stream URI
   *
   * @param flowlet name of the flowlet
   * @param stream name of the stream
   * @param uri URI associated with the stream.
   */
  public void setStreamURI(String flowlet, String stream, URI uri, StreamType type);
}

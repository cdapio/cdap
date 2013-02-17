package com.continuuity.internal.app.services.legacy;

import com.google.common.base.Objects;

/**
 * Implementation of FlowletStreamDefinition.
 *
 * A FlowletStreamDefinition specifies an endpoint of an edge or connection in
 * a flow.
 */
public class FlowletStreamDefinitionImpl implements FlowletStreamDefinition {
  private String flowlet;
  private String stream;

  /**
   * Empty constructor of this object.
   */
  public FlowletStreamDefinitionImpl() {
  }

	/**
	 * Constructor of FlowStreamDefinition by specifying the a stream of a flowlet.
	 * @param flowlet name of the flowlet
	 * @param stream  name of the flowlet's stream
	 */
	public FlowletStreamDefinitionImpl(String flowlet, String stream) {
		this.flowlet = flowlet;
		this.stream = stream;
	}

	/**
	 * Constructor of FlowStreamDefinition by specifing a stream of the flow`.
	 * @param stream  name of the stream
	 */
	public FlowletStreamDefinitionImpl(String stream) {
		this.flowlet = null;
		this.stream = stream;
	}

	@Override
  public String getFlowlet() {
    return flowlet;
  }

	@Override
  public String getStream() {
    return stream;
  }

	@Override
	public boolean isFlowStream() {
		return this.flowlet == null;
	}

	@Override
	public boolean isFlowletStream() {
		return this.flowlet != null;
	}

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("flowlet", flowlet)
        .add("stream", stream)
        .toString();
  }
}

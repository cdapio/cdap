package com.continuuity.internal.app.services.legacy;

import java.net.URI;

/**
 * FlowInputDefinition provides the stream parameters of the flow. These
 * are external streams, that is they are not written (or not read) by
 * the flowlets within this flow, but by some other entity outside this
 * flow, such as the gateway or another flow.
 *
 * A flow stream has a name, by which is referenced within the flow, and a
 * URI, which is used to address the stream in the data fabric. The name
 * must be unique within the flow's streams, whereas the URI must be unique
 * across flows.
 */
public interface FlowStreamDefinition {
	/**
	 * Returns the name of the stream
	 * @return name of the stream.
	 */
	public String getName();

	/**
	 * Returns the URI of the stream.
	 * @return the URI of the stream.
	 */
	public URI getURI();
}

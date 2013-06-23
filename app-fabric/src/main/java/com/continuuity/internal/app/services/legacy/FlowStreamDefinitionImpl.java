package com.continuuity.internal.app.services.legacy;

import java.net.URI;

/**
 * Implementation of @link{FlowStreamDefinition}.
 */
public class FlowStreamDefinitionImpl implements FlowStreamDefinition {

	/** the name of the stream */
	String name;
	/** the URI of the stream */
	URI uri;

	/**
	 * Empty constructor of this object.
	 */
	public FlowStreamDefinitionImpl() {
	}

	/**
	 * Constructor from name and uri
	 * @param name the name of the stream
	 * @param uri the URI of the stream
	 */
	public FlowStreamDefinitionImpl(String name, URI uri) {
		this.name = name;
		this.uri = uri;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public URI getURI() {
		return uri;
	}
}
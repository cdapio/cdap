/*
 * Copyright (c) 2012, Continuuity Inc. All rights reserved.
 */

package com.continuuity.gateway;

import org.apache.hadoop.conf.Configuration;

/**
 *
 */
public abstract class Connector {

	protected String name;
	protected Consumer consumer;
	protected Configuration configuration;

	public void setConsumer(Consumer consumer) {
		this.consumer = consumer;
	}
	public Consumer getConsumer() {
		return  this.consumer;
	}

	public void setName(String name) {
		this.name = name;
	}
	public String getName() {
		return this.name;
	}

	public void configure(Configuration configuration) throws Exception {
		this.configuration = configuration;
	}

	public abstract void start() throws Exception;
	public abstract void stop() throws Exception;
}

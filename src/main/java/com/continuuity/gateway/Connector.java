/*
 * Copyright (c) 2012, Continuuity Inc. All rights reserved.
 */

package com.continuuity.gateway;

import org.apache.hadoop.conf.Configuration;

/**
 * Created by: andreas, having fun since April 2012
 */
public abstract class Connector {

	protected Consumer consumer;

	public void setConsumer(Consumer consumer) {
		this.consumer = consumer;
	}
	public Consumer getConsumer() {
		return  this.consumer;
	}

	public abstract void configure(Configuration configuration);
	public abstract void start();
	public abstract void stop();
}

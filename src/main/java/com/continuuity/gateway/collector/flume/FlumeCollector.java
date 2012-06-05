/*
 * Copyright (c) 2012, Continuuity Inc. All rights reserved.
 */

package com.continuuity.gateway.collector.flume;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.gateway.Collector;
import com.continuuity.gateway.Constants;
import com.continuuity.gateway.Consumer;

/**
 *
 */
public abstract class FlumeCollector extends Collector {

	public static final int DefaultPort = 8765;

	protected int port = DefaultPort;
	protected FlumeAdapter flumeAdapter;

	@Override
	public void setConsumer(Consumer consumer) {
		super.setConsumer(consumer);
		if (this.flumeAdapter == null) {
			this.flumeAdapter = new FlumeAdapter(this);
		}
		this.flumeAdapter.setConsumer(consumer);
	}

	@Override
	public Consumer getConsumer() {
		if (this.flumeAdapter == null) return null;
		return this.flumeAdapter.getConsumer();
	}

	@Override
	public void configure(CConfiguration configuration) throws Exception {
		super.configure(configuration);
		this.port = configuration.getInt(Constants.buildCollectorPropertyName(
				this.name, Constants.CONFIG_PORT), DefaultPort);
	}

	public int getPort() {
		return this.port;
	}

	public String toString() {
		return this.getClass().getName() + " at :" + this.getPort() + " (" +
				(this.consumer == null ? "no consumer set" : this.consumer.getClass().getName()) + ")";
	}
}

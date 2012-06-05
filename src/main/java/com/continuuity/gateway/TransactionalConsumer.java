package com.continuuity.gateway;

import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.TransactionalOperationExecutor;
import com.continuuity.flow.flowlet.api.Event;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class TransactionalConsumer extends Consumer {

  /**
   * This is the operations executor that we will use to talk to the data-fabric
   */
  @Inject
  private OperationExecutor theExecutor;

	/** This is our Logger class */
	private static final Logger LOG = LoggerFactory
			.getLogger(TransactionalConsumer.class);


	protected void single(Event event) throws Exception {
		TransactionalOperationExecutor executor = null; // new TransactionalOperationExecutor()
	}

	protected void batch(List<Event> events) throws Exception {
		for (Event event : events) {
			this.single(event);
		}
	}


}

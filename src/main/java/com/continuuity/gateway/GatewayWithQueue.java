/*
 * Copyright (c) 2012, Continuuity Inc. All rights reserved.
 */

package com.continuuity.gateway;

import com.continuuity.data.engine.memory.MemoryQueueTable;
import com.continuuity.data.operation.queue.QueueConfig;
import com.continuuity.data.operation.queue.QueueConsumer;
import com.continuuity.data.operation.queue.QueueEntry;
import com.continuuity.data.operation.queue.QueuePartitioner;
import com.continuuity.flow.flowlet.api.Event;
import com.continuuity.flow.flowlet.impl.EventSerializer;

/**
 * Created by: andreas, having fun since April 2012
 */
public class GatewayWithQueue {

	private static final int defaultSecondsToLive = 10;

	private static void usage(boolean isError) {
		System.err.println("Usage: GatewayWithQueue [ --host <host> ] [ --port <number> ]");
		System.err.println("        [ --live <seconds> ] [ --verbose ] [ --interval <count> ]");
		System.exit(isError ? 1 : 0);
	}

	public static void main(String args[]) throws Exception {
		String hostname = null;
		Integer port = null;
		int interval = 1000;
		boolean verbose = false;
		int secondsToLive = defaultSecondsToLive;

		int pos = 0;
		while (pos < args.length) {
			String arg = args[pos++];
			if ("--help".equals(arg)) {
				usage(false);
			}
			else if ("--verbose".equals(arg)) {
				verbose = true;
			}
			else if ("--live".equals(arg)) {
				if (pos >= args.length) usage(true);
				try {
					secondsToLive = Integer.valueOf(args[pos++]);
				} catch (NumberFormatException e) {
					usage(true);
				}
			}
			else if ("--interval".equals(arg)) {
				if (pos >= args.length) usage(true);
				try {
					interval = Integer.valueOf(args[pos++]);
				} catch (NumberFormatException e) {
					usage(true);
				}
			}
			else if ("--port".equals(arg)) {
				if (pos >= args.length) usage(true);
				try {
					port = Integer.valueOf(args[pos++]);
				} catch (NumberFormatException e) {
					usage(true);
				}
			}
			else if ("--host".equals(arg)) {
				if (pos >= args.length) usage(true);
				hostname = args[pos++];
			}
		}

		FlumeConnector connector = null;
		try {
			connector = new NettyFlumeConnector();
		} catch (Exception e) {
			System.err.println("Error instantiating FlumeConnector:");
			e.printStackTrace(System.err);
			usage(true);
		}
		if (hostname != null) connector.setHost(hostname);
		if (port != null) connector.setPort(port);

		MemoryQueueTable queues = new MemoryQueueTable();
		QueueWritingConsumer queueWriter = new QueueWritingConsumer(queues);
		connector.setConsumer(queueWriter);

		QueueConsumer consumer = new QueueConsumer(0, 0, 1, true, false);
		QueueConfig config = new QueueConfig(new QueuePartitioner.RandomPartitioner(), true);
		EventSerializer deserializer = new EventSerializer();

		queueWriter.startConsumer();
		connector.start();

		long currentTime = System.currentTimeMillis();
		long startTime = currentTime;
		long endTime =  currentTime + 1000 * secondsToLive;

		long count = 0;
		while(currentTime < endTime) {
			QueueEntry entry = queues.pop("default".getBytes(), consumer, config, false);
			byte[] bytes = entry.getValue();
			Event event =	deserializer.deserialize(bytes);
			if (event.getBody().length < 0) {
				System.out.println("Received event with negative length " + event.getBody().length);
			}
			queues.ack("default".getBytes(), entry);
			currentTime = System.currentTimeMillis();
			++count;
			if (verbose) System.out.println("Popped one: " + entry);
			if (interval != 0 && count % interval == 0)
				System.out.println("Popped " + count + " entries in " +
						(currentTime - startTime) + " ms");
		}

		connector.stop();
		queueWriter.stopConsumer();

		long eventsPushed = queueWriter.eventsSucceeded();
		while(count < eventsPushed) {
			QueueEntry entry = queues.pop("default".getBytes(), consumer, config, false);
			queues.ack("default".getBytes(), entry);
			currentTime = System.currentTimeMillis();
			++count;
			if (verbose) System.out.println("Popped one: " + entry);
			if (interval != 0 && count % interval == 0)
				System.out.println("Popped " + count + " entries in " +
						(currentTime - startTime) + " ms");
		}

		System.out.println("Popped " + count + " queue entries");
	}

}

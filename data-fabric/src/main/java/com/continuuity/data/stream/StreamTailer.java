/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream;

import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data.runtime.DataSetsModules;
import com.continuuity.data2.transaction.stream.StreamAdmin;
import com.continuuity.data2.transaction.stream.StreamConfig;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.filesystem.Location;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Command line tool for tailing stream.
 */
public class StreamTailer {

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      System.out.println(String.format("Usage: java %s [streamName]", StreamTailer.class.getName()));
      return;
    }
    String streamName = args[0];

    CConfiguration cConf = CConfiguration.create();
    Configuration hConf = new Configuration();

    Injector injector = Guice.createInjector(new ConfigModule(cConf, hConf),
                                             new DataFabricModules().getDistributedModules(),
                                             new DataSetsModules().getDistributedModule(),
                                             new LocationRuntimeModule().getDistributedModules());

    StreamAdmin streamAdmin = injector.getInstance(StreamAdmin.class);
    StreamConfig streamConfig = streamAdmin.getConfig(streamName);
    Location streamLocation = streamConfig.getLocation();
    List<Location> eventFiles = Lists.newArrayList();

    for (Location partition : streamLocation.list()) {
      if (!partition.isDirectory()) {
        continue;
      }

      for (Location file : partition.list()) {
        if (StreamFileType.EVENT.isMatched(file.getName())) {
          eventFiles.add(file);
        }
      }
    }

    int generation = StreamUtils.getGeneration(streamConfig);
    MultiLiveStreamFileReader reader = new MultiLiveStreamFileReader(streamConfig,
      ImmutableList.copyOf(Iterables.transform(eventFiles, createOffsetConverter(generation))));
    List<StreamEvent> events = Lists.newArrayList();
    while (reader.read(events, 10, 100, TimeUnit.MILLISECONDS) >= 0) {
      for (StreamEvent event : events) {
        System.out.println(event.getTimestamp() + " " + Charsets.UTF_8.decode(event.getBody()));
      }
      events.clear();
    }

    reader.close();
  }

  private static Function<Location, StreamFileOffset> createOffsetConverter(final int generation) {
    return new Function<Location, StreamFileOffset>() {
      @Override
      public StreamFileOffset apply(Location eventLocation) {
        return new StreamFileOffset(eventLocation, 0L, generation);
      }
    };
  }
}

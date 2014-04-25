/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream;

import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data2.transaction.stream.StreamAdmin;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.filesystem.Location;

import java.util.List;
import java.util.Map;
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
                                             new DataFabricModules(cConf, hConf).getDistributedModules(),
                                             new LocationRuntimeModule().getDistributedModules());

    StreamAdmin streamAdmin = injector.getInstance(StreamAdmin.class);
    Location streamLocation = streamAdmin.getConfig(streamName).getLocation();
    Map<String, StreamFile> streamFiles = Maps.newHashMap();

    for (Location partition : streamLocation.list()) {
      if (!partition.isDirectory()) {
        continue;
      }

      for (Location file : partition.list()) {
        int idx = file.toURI().toString().lastIndexOf('.');
        if (idx > 0) {
          String name = file.toURI().toString().substring(0, idx);
          StreamFile streamFile = streamFiles.get(name);
          if (streamFile == null) {
            streamFile = new StreamFile();
            streamFiles.put(name, streamFile);
          }
          if (file.getName().endsWith(StreamFileType.EVENT.getSuffix())) {
            streamFile.setEventLocation(file);
          } else if (file.getName().endsWith(StreamFileType.INDEX.getSuffix())) {
            streamFile.setIndexLocation(file);
          }
        }
      }
    }

    System.out.println(streamFiles);

    MultiLiveStreamFileReader reader = new MultiLiveStreamFileReader(streamAdmin.getConfig(streamName),
      ImmutableList.copyOf(Iterables.transform(streamFiles.values(), createOffsetConverter())));
    List<StreamEvent> events = Lists.newArrayList();
    while (reader.read(events, 10, 100, TimeUnit.MILLISECONDS) >= 0) {
      for (StreamEvent event : events) {
        System.out.println(event.getTimestamp() + " " + Charsets.UTF_8.decode(event.getBody()));
      }
      events.clear();
    }

    reader.close();
  }

  private static final class StreamFile {

    private Location eventLocation;
    private Location indexLocation;

    public Location getEventLocation() {
      return eventLocation;
    }

    public void setEventLocation(Location eventLocation) {
      this.eventLocation = eventLocation;
    }

    public Location getIndexLocation() {
      return indexLocation;
    }

    public void setIndexLocation(Location indexLocation) {
      this.indexLocation = indexLocation;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("event", eventLocation.toURI())
        .add("index", indexLocation.toURI())
        .toString();
    }
  }

  private static Function<StreamFile, StreamFileOffset> createOffsetConverter() {
    return new Function<StreamFile, StreamFileOffset>() {
      @Override
      public StreamFileOffset apply(StreamFile input) {
        return new StreamFileOffset(input.getEventLocation(), input.getIndexLocation());
      }
    };
  }
}

/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream;

import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.filesystem.Location;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Command line tool for tailing stream.
 */
public class StreamTailer {

  public static void main(String[] args) throws IOException, InterruptedException {
    if (args.length < 1) {
      System.out.println(String.format("Usage: java %s [streamName]", StreamTailer.class.getName()));
      return;
    }
    String streamName = args[0];

    CConfiguration cConf = CConfiguration.create();
    Configuration hConf = new Configuration();

    Injector injector = Guice.createInjector(new ConfigModule(cConf, hConf),
                                             new LocationRuntimeModule().getDistributedModules());

    StreamManager manager = injector.getInstance(StreamManager.class);
    Location streamLocation = manager.getStreamLocation(streamName);
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

    MultiStreamDataFileReader reader = new MultiStreamDataFileReader(streamFiles.values(),
                                                                     new StreamFileReaderFactory());
    List<StreamEvent> events = Lists.newArrayList();
    while (reader.read(events, 10, 100, TimeUnit.MILLISECONDS) >= 0) {
      for (StreamEvent event : events) {
        System.out.println(event.getTimestamp() + " " + Charsets.UTF_8.decode(event.getBody()));
      }
      events.clear();
    }

    reader.close();
  }

  private static final class StreamFile extends StreamFileOffset {

    private Location eventLocation;
    private Location indexLocation;

    public StreamFile() {
      super(null, null);
    }

    @Override
    public Location getEventLocation() {
      return eventLocation;
    }

    public void setEventLocation(Location eventLocation) {
      this.eventLocation = eventLocation;
    }

    @Override
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
}

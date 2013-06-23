package com.continuuity.common.logging;

import com.continuuity.common.conf.CConfiguration;
import org.apache.avro.AvroRemoteException;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.flume.source.avro.AvroSourceProtocol;
import org.apache.flume.source.avro.Status;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TBaseHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FlumeLogAdapter implements AvroSourceProtocol {

  private static final Logger LOG
    = LoggerFactory.getLogger(FlumeLogAdapter.class);
  private LogCollector collector;

  public FlumeLogAdapter(CConfiguration config, Configuration hConfig) throws IOException {
    this.collector = new LogCollector(config, hConfig);
  }

  private static Map<String, String>
  toStringMap(Map<CharSequence, CharSequence> charSeqMap) {
    Map<String, String> stringMap =
      new HashMap<String, String>();
    if(charSeqMap == null) {
      return stringMap;
    }
    for(Map.Entry<CharSequence, CharSequence> entry : charSeqMap.entrySet()) {
      stringMap.put(entry.getKey().toString(), entry.getValue().toString());
    }
    return stringMap;
  }

  @Override
  public Status append(AvroFlumeEvent event) throws AvroRemoteException {
    Map<String, String> headers = toStringMap(event.getHeaders());
    if(! headers.containsKey(LogEvent.FIELD_NAME_LOGTAG) ||
       ! headers.containsKey(LogEvent.FIELD_NAME_LOGLEVEL)) {
      return Status.UNKNOWN;
    }

    String logtag = headers.get(LogEvent.FIELD_NAME_LOGTAG);
    String level = headers.get(LogEvent.FIELD_NAME_LOGLEVEL);

    if(logtag == null || logtag.isEmpty() || level == null || level.isEmpty()) {
      LOG.warn("Logtag or level is null. Please check the send events.");
      return Status.UNKNOWN;
    }

    String body = new String(TBaseHelper.byteBufferToByteArray(event.getBody()));
    LogEvent logEvent = new LogEvent(logtag, level, body);
    this.collector.log(logEvent);
    return Status.OK;
  }

  @Override
  public Status appendBatch(List<AvroFlumeEvent> events) throws AvroRemoteException {
    Status status = Status.OK;
    for (AvroFlumeEvent event : events) {
      Status stat = append(event);
      if (stat.equals(Status.FAILED) ||
          stat.equals(Status.UNKNOWN) && (!status.equals(Status.FAILED)))
        status = stat;
    }
    return status;
  }
}

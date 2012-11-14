package com.continuuity.common.logging;

import com.continuuity.common.conf.CConfiguration;
import org.apache.avro.AvroRemoteException;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.flume.source.avro.AvroSourceProtocol;
import org.apache.flume.source.avro.Status;
import org.apache.thrift.TBaseHelper;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class FlumeLogAdapter implements AvroSourceProtocol {

  private CConfiguration config;
  private LogCollector collector;

  public FlumeLogAdapter(CConfiguration config) throws IOException {
    this.config = config;
    this.collector = new LogCollector(config);
  }

  @Override
  public Status append(AvroFlumeEvent event) throws AvroRemoteException {
    if(event == null) {
      return Status.UNKNOWN;
    }
    Map<CharSequence, CharSequence> headers = event.getHeaders();
    if(headers == null) {
      return Status.UNKNOWN;
    }
    String logtag = headers.get(LogEvent.FIELD_NAME_LOGTAG).toString();
    String level = headers.get(LogEvent.FIELD_NAME_LOGLEVEL).toString();
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
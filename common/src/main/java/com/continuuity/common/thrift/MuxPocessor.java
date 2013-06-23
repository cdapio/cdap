package com.continuuity.common.thrift;

import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;

import java.util.HashMap;
import java.util.Map;

/**
 * <code>MuxPocessor</code> is a <code>TProcessor</code> allowing
 * a single <code>TServer</code> to provide multiple services.
 *
 * <p>To do so, you instantiate the processor and then register additional
 * processors with it, as shown in the following example:</p>
 *
 * <blockquote>
 *   <code>
 *     MuxPocessor processor = new MuxPocessor();
 *
 *     processor.registerProcessor(
 *         "MetricsService",
 *         new MetricsService.Processor(new MetricsServiceHandler()));
 *
 *     processor.registerProcessor(
 *         "MetaDataService",
 *         new MetaDataService.Processor(new MetaDataServiceHandler()));
 *
 *     TServerTransport t = new TServerSocket(9090);
 *     THsHaServer server = new THsHaServer(processor, t);
 *     server.serve();
 *   </code>
 * </blockquote>
 */
public class MuxPocessor implements TProcessor {

  private final Map<String,TProcessor> SERVICE_PROCESSOR_MAP
    = new HashMap<String,TProcessor>();

  /**
   * 'Register' a service with this <code>MuxPocessor</code>.  This
   * allows us to broker requests to individual services by using the service
   * name to select them at request time.
   *
   * @param serviceName Name of a service, has to be identical to the name
   * declared in the Thrift IDL, e.g. "WeatherReport".
   * @param processor Implementation of a service, ususally referred to
   * as "handlers", e.g. WeatherReportHandler implementing WeatherReport.Iface.
   */
  public void registerProcessor(String serviceName, TProcessor processor) {
    SERVICE_PROCESSOR_MAP.put(serviceName, processor);
  }

  /**
   * This implementation of <code>process</code> performs the following steps:
   *
   * <ol>
   *     <li>Read the beginning of the message.</li>
   *     <li>Extract the service name from the message.</li>
   *     <li>Using the service name to locate the appropriate processor.</li>
   *     <li>Dispatch to the processor, with a decorated instance of TProtocol
   *         that allows readMessageBegin() to return the original TMessage.</li>
   * </ol>
   *
   * @throws TException If the message type is not CALL or ONEWAY, if
   * the service name was not found in the message, or if the service
   * name was not found in the service map.  You called
   * {@link #registerProcessor(String, TProcessor) registerProcessor}
   * during initialization, right? :)
   */
  @Override
  public boolean process(TProtocol iprot, TProtocol oprot) throws TException {
    // Use the actual underlying protocol (e.g. TBinaryProtocol) to read the
    // message header.  This pulls the message "off the wire", which we'll
    // deal with at the end of this method.
    TMessage message = iprot.readMessageBegin();

    if (message.type != TMessageType.CALL
      && message.type != TMessageType.ONEWAY) {
      throw new TException("This should not have happened!?");
    }

    // Extract the service name
    int index = message.name.indexOf(MuxProtocol.SEPARATOR);
    if (index < 0) {
      throw new TException("Service name not found in message name: " +
        message.name + ".  Make sure to use a TMultiplexProtocol in " +
        "your client?");
    }

    // Create a new TMessage, something that can be consumed by any TProtocol
    String serviceName = message.name.substring(0, index);
    TProcessor actualProcessor = SERVICE_PROCESSOR_MAP.get(serviceName);
    if (actualProcessor == null) {
      throw new TException("Service name not found: " + serviceName +
        ".  Check if you" + "call registerProcessor()?");
    }

    // Create a new TMessage, removing the service name
    TMessage standardMessage = new TMessage(
      message.name.substring(
        serviceName.length() +
        MuxProtocol.SEPARATOR.length()
      ),
      message.type,
      message.seqid
    );

    // Dispatch processing to the stored processor
    return actualProcessor.process(
      new StandardProtocol(iprot, standardMessage), oprot);
  }

  /**
   * In order to work with any protocol, we need to get the TMessage in exactly
   * the standard thrift format without service name to the processor.
   */
  private class StandardProtocol extends ProtocolDelegation {
    TMessage messageBegin;
    public StandardProtocol(TProtocol protocol, TMessage messageBegin) {
      super(protocol);
      this.messageBegin = messageBegin;
    }
    @Override
    public TMessage readMessageBegin() throws TException {
      return messageBegin;
    }
  }

}

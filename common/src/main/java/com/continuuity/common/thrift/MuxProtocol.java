package com.continuuity.common.thrift;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;

/**
 * <code>MuxProtocol</code> is a protocol-independent concrete decorator
 * that allows a Thrift client to communicate with a multiplexing Thrift server,
 * by prepending the service name to the function name during function calls.
 *
 * <p>NOTE: THIS IS NOT USED BY SERVERS.  On the server, use
 * {@link MuxPocessor MuxPocessor} to handle requests
 * from a multiplexing client.
 *
 * <p>This example uses a single socket transport to invoke two services:
 *
 * <blockquote><code>
 *     TSocket transport = new TSocket("localhost", 9090);<br/>
 *     transport.open();<br/>
 *<br/>
 *     TBinaryProtocol protocol = new TBinaryProtocol(transport);<br/>
 *<br/>
 *     MuxProtocol mp
 *      = new MuxProtocol(protocol, "MetricsService");<br/>
 *     MetricsService.Client mts = new MetricsService.Client(mp);<br/>
 *<br/>
 *     MuxProtocol mp2
 *      = new MuxProtocol(protocol, "MetaDataService");<br/>
 *     MetaDataService.Client mds = new MetaDataService.Client(mp2);<br/>
 *<br/>
 *     System.out.println(mts.getTimeSeries('processed.count'));<br/>
 *     System.out.println(mds.getStreams());<br/>
 * </code></blockquote>
 *
 * @see ProtocolDelegation
 */
public class MuxProtocol extends ProtocolDelegation {

  /** Used to delimit the service name from the function name */
  public static final String SEPARATOR = ".";

  private final String serviceName;

  /**
   * Wrap the specified protocol, allowing it to be used to communicate with a
   * multiplexing server.  The <code>serviceName</code> is required as it is
   * prepended to the message header so that the multiplexing server can broker
   * the function call to the proper service.
   *
   * @param protocol Your communication protocol of choice,
   *                 e.g. <code>TBinaryProtocol</code>.
   * @param serviceName The service name of the service communicating via
   *                    this protocol.
   */
  public MuxProtocol(TProtocol protocol, String serviceName) {
    super(protocol);
    this.serviceName = serviceName;
  }

  /**
   * Prepends the service name to the function name, separated by
   * MuxProtocol.SEPARATOR.
   *
   * @param tMessage The original message.
   * @throws TException Passed through from wrapped
   * <code>TProtocol</code> instance.
   */
  @Override
  public void writeMessageBegin(TMessage tMessage) throws TException {
    if (tMessage.type == TMessageType.CALL
        || tMessage.type == TMessageType.ONEWAY) {
      super.writeMessageBegin(new TMessage(
        serviceName + SEPARATOR + tMessage.name,
        tMessage.type,
        tMessage.seqid
      ));
    } else {
      super.writeMessageBegin(tMessage);
    }
  }
}

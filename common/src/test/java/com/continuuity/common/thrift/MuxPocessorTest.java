package com.continuuity.common.thrift;

import com.continuuity.common.thrift.stubs.AddingService;
import com.continuuity.common.thrift.stubs.MultiplyingService;
import com.continuuity.common.thrift.stubs.SubtractingService;
import com.continuuity.common.utils.PortDetector;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.transport.*;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;

/**
 * MuxProcessor test for testing multiple service serviced by a single server.
 */
public class MuxPocessorTest {

  /** Implementation of service handler for addition */
  public static class AddingServiceHandler implements AddingService.Iface {
    @Override
    public long add(int num1, int num2) throws TException {
      return num1 + num2;
    }
  }

  /** Implementation of service handler for substraction */
  public static class SubtractingServiceHandler
      implements SubtractingService.Iface {
    @Override
    public long sub(int num1, int num2) throws TException {
      return num1 - num2;
    }
  }

  /** Implementation of service handler for multiplying */
  public static class MultiplyingServiceHandler
    implements MultiplyingService.Iface {
    @Override
    public long multiply(int num1, int num2) throws TException {
      return num1 * num2;
    }
  }

  /**
   * Tests running multiple services under a single server. Test verifies
   * the implementation of MuxProcessor and MuxProtocol.
   *
   * @throws Exception
   */
  @Test
  public void testMuxProcessor() throws Exception {
    // Create a mux processor on the server.
    MuxPocessor muxPocessor = new MuxPocessor();

    // Add multiple services to the MuxProcessor.
    muxPocessor.registerProcessor("AddingService",
                  new AddingService.Processor<AddingServiceHandler>(
                    new AddingServiceHandler()
                  ));
    muxPocessor.registerProcessor("SubtractingService",
                 new SubtractingService.Processor<SubtractingServiceHandler>(
                  new SubtractingServiceHandler()
                 ));
    muxPocessor.registerProcessor("MultiplyingService",
                  new MultiplyingService.Processor<MultiplyingServiceHandler>(
                    new MultiplyingServiceHandler()
                  ));

    // Find the next available free port.
    int port = PortDetector.findFreePort();

    // Create the HsHa Server Args.
    THsHaServer.Args serverArgs =
      new THsHaServer
        .Args(new TNonblockingServerSocket(new InetSocketAddress(
          "localhost",
          port
      ))).processor(muxPocessor);

    // Server read buffer limit (to prevent from OOME)
    serverArgs.maxReadBufferBytes = 1048576;

    // Start the server
    final THsHaServer server = new THsHaServer(serverArgs);
    Assert.assertNotNull(server); // server should not be null.

    // Start the server within the thread, so that we don't block.
    new Thread(new Runnable() {
      @Override
      public void run() {
        server.serve();
      }
    }).start();

    // Create a framed transport.
    TTransport transport = new TFramedTransport(
      new TSocket("localhost", port)
    );

    // Open the transport.
    transport.open();

    // Bind the transport to the protocol.
    TBinaryProtocol protocol = new TBinaryProtocol(transport);

    MuxClientProtocol muxClientProtocol
      = new MuxClientProtocol(protocol, "AddingService", "SubtractingService");
    Assert.assertNotNull(muxClientProtocol);

    // Add another service with same protocol. You can add more
    // services with different protocol.
    muxClientProtocol.add(protocol, "MultiplyingService");

    // Get a adding service client.
    AddingService.Client addingServiceClient =
      new AddingService.Client(muxClientProtocol.get("AddingService"));
    Assert.assertNotNull(addingServiceClient);

    // Test adding.
    long addResult = addingServiceClient.add(10, 12);
    Assert.assertTrue(addResult == 22);

    // Get a subtracting service client.
    SubtractingService.Client substractingServiceClient =
      new SubtractingService.Client(
        muxClientProtocol.get("SubtractingService")
      );
    Assert.assertNotNull(substractingServiceClient);

    // Test subtracting
    long subResult = substractingServiceClient.sub(12, 10);
    Assert.assertTrue(subResult == 2);

    // Get a multiplying service client.
    MultiplyingService.Client multiplyingServiceClient =
      new MultiplyingService.Client(
        muxClientProtocol.get("MultiplyingService")
      );
    Assert.assertNotNull(multiplyingServiceClient);

    // Test Multiplying call
    long multiplyResult = multiplyingServiceClient.multiply(10, 2);
    Assert.assertTrue(multiplyResult == 20);

    // Stop the server.
    server.stop();
  }
}

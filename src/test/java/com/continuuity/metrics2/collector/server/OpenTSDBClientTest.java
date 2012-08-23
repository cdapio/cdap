package com.continuuity.metrics2.collector.server;

import org.apache.mina.core.future.IoFutureListener;
import org.apache.mina.core.future.WriteFuture;
import org.junit.Test;

/**
 * Testing of opentsdb client.
 */
public class OpenTSDBClientTest {

//  @Test
//  public void testTSDBClientConnect() throws Exception {
//    OpenTSDBClient client = new OpenTSDBClient("localhost", 4242);
//
//    client.send("put nitin.loadavg.1m 1288946927 0.36 host=foo").addListener(
//      new IoFutureListener<WriteFuture>() {
//        @Override
//        public void operationComplete(WriteFuture future) {
//          if(! future.isWritten()) {
//            System.err.println("FAILED");
//          } else {
//            System.err.println("SUCCESS");
//          }
//        }
//      }
//    );
//
//    Thread.sleep(10000);
//  }
}

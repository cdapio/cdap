package com.continuuity.common.http.core;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Service;

import java.io.File;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 *
 */
public class WebCloudAppServer {

  private final List<HttpHandler> handlers = Lists.newArrayList();
  private NettyHttpService service;
  private final CountDownLatch running = new CountDownLatch(1);

  private void run(String[] args) {
    NettyHttpService.Builder builder = NettyHttpService.builder();
    builder.addHttpHandlers(handlers).setDocumentRoot(
      new File(System.getProperty("user.dir") + File.separator + "web-app/client")
    );

    service = builder.build();
    service.startAndWait();
    Service.State state = service.state();
    System.out.println("Service started on " + service.getBindAddress().getPort());
    try {
      running.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  public static void main(String[] args) {
    new WebCloudAppServer().run(args);
  }

}

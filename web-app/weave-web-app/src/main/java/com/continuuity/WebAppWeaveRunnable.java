package com.continuuity;

import com.continuuity.common.http.core.HttpHandler;
import com.continuuity.common.http.core.NettyHttpService;
import com.continuuity.weave.api.AbstractWeaveRunnable;
import com.continuuity.weave.api.WeaveContext;
import com.continuuity.weave.api.WeaveRunnableSpecification;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Runnable for WebApp.
 */
public class WebAppWeaveRunnable extends AbstractWeaveRunnable {
  private static final Logger LOG = LoggerFactory.getLogger(WebAppWeaveRunnable.class);
  private final List<HttpHandler> handlers = Lists.newArrayList();
  private NettyHttpService service;
  private CountDownLatch running;

  @Override
  public WeaveRunnableSpecification configure() {
    return WeaveRunnableSpecification.Builder.with()
      .setName("web-app")
      .noConfigs().build();
  }

  @Override
  public void initialize(WeaveContext context) {
    super.initialize(context);

    NettyHttpService.Builder builder = NettyHttpService.builder();
    builder.addHttpHandlers(handlers).setDocumentRoot(
      new File(System.getProperty("user.dir") + File.separator + "web-app/client")
    );

    service = builder.build();
    running = new CountDownLatch(1);
  }

  @Override
  public void run() {
    service.startAndWait();
    try {
      running.await();
    } catch (InterruptedException e) {
      LOG.warn("WebAppWeaveRunnable has been interrupted");
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void stop() {
    service.stopAndWait();
    running.countDown();
  }


}

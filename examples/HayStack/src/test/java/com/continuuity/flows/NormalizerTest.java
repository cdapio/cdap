package com.continuuity.flows;

import com.continuuity.meta.Event;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class NormalizerTest {
  @Test
  public void testNormalize() throws Exception {
    Event event = Normalizer.parseEvent(ImmutableMap.of("host", "abc.com",
                                                        "component", "app-fabric",
                                                        "logline", log));

    Assert.assertEquals(1380909891489L, event.getTimestamp());
    Assert.assertEquals(log, event.getPayload());
    Assert.assertEquals("abc.com", event.getDimensions().get("hostname"));
    Assert.assertEquals("app-fabric", event.getDimensions().get("component"));
    Assert.assertEquals("WARN", event.getDimensions().get("level"));
    Assert.assertEquals("o.a.h.c.Configuration", event.getDimensions().get("class"));
  }

  @Test
  public void testNormalizeException() throws Exception {
    Event event = Normalizer.parseEvent(ImmutableMap.of("host", "def.com",
                                                        "component", "metrics",
                                                        "logline", logException));

    Assert.assertEquals(1380909895964L, event.getTimestamp());
    Assert.assertEquals(logException, event.getPayload());
    Assert.assertEquals("def.com", event.getDimensions().get("hostname"));
    Assert.assertEquals("metrics", event.getDimensions().get("component"));
    Assert.assertEquals("ERROR", event.getDimensions().get("level"));
    Assert.assertEquals("c.c.m.p.KafkaMetricsProcessingService", event.getDimensions().get("class"));
  }

  @Test
  public void testNormalizeWeave() throws Exception {
    Event event = Normalizer.parseEvent(ImmutableMap.of("host", "w1.com",
                                                        "component", "app-fabric",
                                                        "logline", logWeave));

    Assert.assertEquals(1380846308863L, event.getTimestamp());
    Assert.assertEquals(logWeave, event.getPayload());
    Assert.assertEquals("w1.com", event.getDimensions().get("hostname"));
    Assert.assertEquals("app-fabric", event.getDimensions().get("component"));
    Assert.assertEquals("INFO", event.getDimensions().get("level"));
    Assert.assertEquals("c.c.d.t.d.AbstractClientProvider", event.getDimensions().get("class"));
  }

  @Test
  public void testNormalizeWeaveException() throws Exception {
    Event event = Normalizer.parseEvent(ImmutableMap.of("host", "w1.com",
                                                        "component", "app-fabric",
                                                        "logline", logWeaveException));

    Assert.assertEquals(1380709856165L, event.getTimestamp());
    Assert.assertEquals(logWeaveException, event.getPayload());
    Assert.assertEquals("w1.com", event.getDimensions().get("hostname"));
    Assert.assertEquals("app-fabric", event.getDimensions().get("component"));
    Assert.assertEquals("ERROR", event.getDimensions().get("level"));
    Assert.assertEquals("c.c.c.h.c.HttpResourceModel", event.getDimensions().get("class"));
  }

  private static final String log = "2013-10-04 11:04:51,489 - WARN  [main:o.a.h.c.Configuration@816] - hadoop.native.lib is deprecated. Instead, use io.native.lib.available";

  private static final String logException = "2013-10-04 11:04:55,964 - ERROR [multi-leader-election:c.c.m.p.KafkaMetricsProcessingService@114] - Failed to get offset from meta table. De" +
    "faulting to beginning. [5000] Giving up after tries=0\n" +
    "com.continuuity.api.data.OperationException: [5000] Giving up after tries=0\n" +
    "        at com.continuuity.metrics.process.KafkaConsumerMetaTable.get(KafkaConsumerMetaTable.java:57) ~[watchdog-1.9.1-SNAPSHOT.jar:na]\n" +
    "        at com.continuuity.metrics.process.KafkaMetricsProcessingService.getOffset(KafkaMetricsProcessingService.java:110) [watchdog-1.9.1-S\n" +
    "NAPSHOT.jar:na]\n" +
    "        at com.continuuity.metrics.process.KafkaMetricsProcessingService.subscribe(KafkaMetricsProcessingService.java:92) [watchdog-1.9.1-SN\n" +
    "APSHOT.jar:na]\n" +
    "        at com.continuuity.metrics.process.KafkaMetricsProcessingService.partitionsChanged(KafkaMetricsProcessingService.java:50) [watchdog-\n" +
    "1.9.1-SNAPSHOT.jar:na]\n" +
    "        at com.continuuity.watchdog.election.MultiLeaderElection$2.run(MultiLeaderElection.java:140) [watchdog-1.9.1-SNAPSHOT.jar:na]\n" +
    "        at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:441) [na:1.6.0_34]\n" +
    "        at java.util.concurrent.FutureTask$Sync.innerRun(FutureTask.java:303) [na:1.6.0_34]\n" +
    "        at java.util.concurrent.FutureTask.run(FutureTask.java:138) [na:1.6.0_34]";

  private static final String logWeave = "2013-10-03T17:25:08,863Z INFO  c.c.d.t.d.AbstractClientProvider [poorna-logsaver-ha.dev.continuuity.net] [executor-46] AbstractClientProvider:newClient(AbstractClientProvider.java:125) - Connected to tx service at localhost:15165";

  private static final String logWeaveException = "2013-10-02T03:30:56,165Z ERROR c.c.c.h.c.HttpResourceModel [poorna-logsaver-ha.dev.continuuity.net] [executor-12] HttpResourceModel:handle(HttpResourceModel.java:106) - Error processing path /v2/apps/CountRandom/webapps/poorna-logsaver-ha.dev.continuuity.net%3A20000/start java.lang.reflect.InvocationTargetException\n" +
    "        at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)\n" +
    "        at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:39)\n" +
    "        at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:25)\n" +
    "        at java.lang.reflect.Method.invoke(Method.java:597)\n" +
    "        at com.continuuity.common.http.core.HttpResourceModel.handle(HttpResourceModel.java:99)\n" +
    "        at com.continuuity.common.http.core.HttpResourceHandler.handle(HttpResourceHandler.java:145)\n" +
    "        at com.continuuity.common.http.core.HttpDispatcher.handleRequest(HttpDispatcher.java:43)\n" +
    "        at com.continuuity.common.http.core.HttpDispatcher.messageReceived(HttpDispatcher.java:38)\n" +
    "        at org.jboss.netty.channel.SimpleChannelUpstreamHandler.handleUpstream(SimpleChannelUpstreamHandler.java:70)\n" +
    "        at org.jboss.netty.channel.DefaultChannelPipeline.sendUpstream(DefaultChannelPipeline.java:564)\n" +
    "        at org.jboss.netty.channel.DefaultChannelPipeline$DefaultChannelHandlerContext.sendUpstream(DefaultChannelPipeline.java:791)\n" +
    "        at org.jboss.netty.handler.execution.ChannelUpstreamEventRunnable.doRun(ChannelUpstreamEventRunnable.java:43)\n" +
    "        at org.jboss.netty.handler.execution.ChannelEventRunnable.run(ChannelEventRunnable.java:67)\n" +
    "        at org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor$ChildExecutor.run(OrderedMemoryAwareThreadPoolExecutor.java:314)\n" +
    "        at java.util.concurrent.ThreadPoolExecutor$Worker.runTask(ThreadPoolExecutor.java:886)\n" +
    "        at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:908)\n" +
    "        at java.lang.Thread.run(Thread.java:662)\n";
}

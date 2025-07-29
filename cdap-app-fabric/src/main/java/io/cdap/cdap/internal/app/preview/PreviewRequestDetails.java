package io.cdap.cdap.internal.app.preview;

import io.cdap.cdap.app.preview.PreviewRequest;

/**
 * Class for holding details of a task
 */
public class PreviewRequestDetails {

  //  private final MetricsCollectionService;
  private final PreviewRequest request;
  private final byte[] pollerInfo;

  public PreviewRequest getRequest() {
    return request;
  }

  public byte[] getPollerInfo() {
    return pollerInfo;
  }

  public PreviewRequestDetails(PreviewRequest request, byte[] pollerInfo) {
//    this.metricsCollectionService = metricsCollectionService;
    this.request = request;
    this.pollerInfo = pollerInfo;
  }

//  public void emitMetrics(boolean succeeded) {
//    long time = System.currentTimeMillis() - startTime;
//    Map<String, String> metricTags = new HashMap<>();
//    metricTags.put(Constants.Metrics.Tag.CLASS, Optional.ofNullable(getClassName()).orElse(""));
//    metricTags.put(Constants.Metrics.Tag.STATUS, succeeded ? SUCCESS : FAILURE);
//    metricsCollectionService.getContext(metricTags)
//        .increment(Constants.Metrics.TaskWorker.REQUEST_COUNT, 1L);
//    metricsCollectionService.getContext(metricTags)
//        .gauge(Constants.Metrics.TaskWorker.REQUEST_LATENCY_MS, time);
//  }

}

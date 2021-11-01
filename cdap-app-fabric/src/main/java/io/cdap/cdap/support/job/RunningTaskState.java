package io.cdap.cdap.support.job;

import io.cdap.cdap.support.status.SupportBundleTaskStatus;

import java.util.concurrent.Future;

public class RunningTaskState {
  private Future<SupportBundleTaskStatus> future;
  private Long startTime;

  public synchronized Future<SupportBundleTaskStatus> getFuture() {
    return future;
  }

  public synchronized void setFuture(Future<SupportBundleTaskStatus> future) {
    this.future = future;
  }

  public synchronized Long getStartTime() {
    return startTime;
  }

  public synchronized void setStartTime(Long startTime) {
    this.startTime = startTime;
  }
}

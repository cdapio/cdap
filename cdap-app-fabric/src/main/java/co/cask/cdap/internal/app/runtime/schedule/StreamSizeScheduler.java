/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.internal.app.runtime.schedule;

import co.cask.cdap.api.metrics.MetricDataQuery;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.api.metrics.MetricTimeSeries;
import co.cask.cdap.api.metrics.MetricType;
import co.cask.cdap.api.metrics.TimeValue;
import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.schedule.Schedule;
import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.app.store.StoreFactory;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.exception.NotFoundException;
import co.cask.cdap.common.stream.notification.StreamSizeNotification;
import co.cask.cdap.config.PreferencesStore;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.schedule.StreamSizeSchedule;
import co.cask.cdap.notifications.service.NotificationContext;
import co.cask.cdap.notifications.service.NotificationHandler;
import co.cask.cdap.notifications.service.NotificationService;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * {@link Scheduler} that triggers program executions based on data availability in streams.
 */
@Singleton
public class StreamSizeScheduler implements Scheduler {
  private static final Logger LOG = LoggerFactory.getLogger(StreamSizeScheduler.class);
  private static final int STREAM_POLLING_THREAD_POOL_SIZE = 10;
  private static final int POLLING_AFTER_NOTIFICATION_RETRY = 3;

  private final long pollingDelay;
  private final NotificationService notificationService;
  private final MetricStore metricStore;
  private final StoreFactory storeFactory;
  private final ProgramRuntimeService programRuntimeService;
  private final PreferencesStore preferencesStore;
  private final ConcurrentMap<Id.Stream, StreamSubscriber> streamSubscribers;

  // Key is scheduleId
  private final ConcurrentSkipListMap<String, StreamSubscriber> scheduleSubscribers;

  private Store store;
  private Executor sendPollingInfoExecutor;

  // Used to schedule polling of a stream only after a certain time - the time after which the metrics are updated
  private ScheduledExecutorService pollBookingExecutor;

  // Scheduled executor used to poll stream at regular intervals, by querying the metric system
  private ScheduledExecutorService streamPollingExecutor;

  @Inject
  public StreamSizeScheduler(CConfiguration cConf, NotificationService notificationService, MetricStore metricStore,
                             StoreFactory storeFactory, ProgramRuntimeService programRuntimeService,
                             PreferencesStore preferencesStore) {
    this.pollingDelay = TimeUnit.SECONDS.toMillis(
      cConf.getLong(Constants.Notification.Stream.STREAM_SIZE_SCHEDULE_POLLING_DELAY));
    this.notificationService = notificationService;
    this.metricStore = metricStore;
    this.storeFactory = storeFactory;
    this.programRuntimeService = programRuntimeService;
    this.preferencesStore = preferencesStore;
    this.streamSubscribers = Maps.newConcurrentMap();
    this.scheduleSubscribers = new ConcurrentSkipListMap<String, StreamSubscriber>();
    this.store = null;
  }

  public void start() {
    sendPollingInfoExecutor = Executors.newCachedThreadPool(
      Threads.createDaemonThreadFactory("stream-size-scheduler-%d"));
    streamPollingExecutor = Executors.newScheduledThreadPool(STREAM_POLLING_THREAD_POOL_SIZE,
                                                             Threads.createDaemonThreadFactory("stream-polling-%d"));
    pollBookingExecutor = Executors.newSingleThreadScheduledExecutor(
      Threads.createDaemonThreadFactory("polling-booking-executor"));

    store = storeFactory.create();
  }

  public void stop() {
    for (StreamSubscriber subscriber : streamSubscribers.values()) {
      subscriber.stopAndWait();
    }
    if (pollBookingExecutor != null) {
      pollBookingExecutor.shutdownNow();
    }
    if (streamPollingExecutor != null) {
      streamPollingExecutor.shutdownNow();
    }
  }

  @Override
  public void schedule(Id.Program program, SchedulableProgramType programType, Schedule schedule)
    throws SchedulerException {
    Preconditions.checkArgument(schedule instanceof StreamSizeSchedule,
                                "Schedule should be of type StreamSizeSchedule");
    StreamSizeSchedule streamSizeSchedule = (StreamSizeSchedule) schedule;
    schedule(program, programType, streamSizeSchedule, true, -1, -1, -1, -1, true);
  }

  /**
   * Handle a new {@link StreamSizeSchedule} object in this scheduler.
   *
   * @param program Program that needs to be run
   * @param programType type of program
   * @param streamSizeSchedule Schedule with which the program runs
   * @param active {@code true} if this schedule is active, {@code false} otherwise
   * @param basePollSize size, in bytes, used as the base count for this schedule, or -1 to start counting from
   *                     the current size of events ingested by the stream, as indicated by metrics. Another way
   *                     to see it is: size of the stream during which the schedule last executed the program,
   *                     or -1 if it never happened yet
   * @param basePollTs timestamp, in milliseconds, which matches the time at which {@code basePollSize} was computed.
   *                   -1 indicates to start counting from the current timestamp
   * @param lastRunSize size, in bytes, seen during the last execution of the {@code program}, or -1 if it never
   *                    happened yet
   * @param lastRunTs timestamp, in milliseconds, at which the {@code lastRunSize} was computed. -1 indicates that
   *                  the {@code program} was never executed via this {@code streamSizeSchedule} before
   * @param persist {@code true} if this schedule should be persisted in the persistent store containing the
   *                stream size schedules, {@code false} otherwise
   */
  private void schedule(Id.Program program, SchedulableProgramType programType, StreamSizeSchedule streamSizeSchedule,
                        boolean active, long basePollSize, long basePollTs, long lastRunSize, long lastRunTs,
                        boolean persist) throws SchedulerException {
    // Create a new StreamSubscriber, if one doesn't exist for the stream passed in the schedule
    Id.Stream streamId = Id.Stream.from(program.getNamespaceId(), streamSizeSchedule.getStreamName());
    StreamSubscriber streamSubscriber = streamSubscribers.get(streamId);
    if (streamSubscriber == null) {
      streamSubscriber = new StreamSubscriber(streamId);
      StreamSubscriber previous = streamSubscribers.putIfAbsent(streamId, streamSubscriber);
      if (previous == null) {
        streamSubscriber.startAndWait();
      } else {
        streamSubscriber = previous;
      }
    }

    // Add the scheduleTask to the StreamSubscriber
    streamSubscriber.createScheduleTask(program, programType, streamSizeSchedule, active,
                                        basePollSize, basePollTs, lastRunSize, lastRunTs, persist);
    scheduleSubscribers.put(AbstractSchedulerService.scheduleIdFor(program, programType,
                                                                   streamSizeSchedule.getName()),
                            streamSubscriber);
  }

  @Override
  public void schedule(Id.Program program, SchedulableProgramType programType, Iterable<Schedule> schedules)
    throws SchedulerException {
    for (Schedule s : schedules) {
      schedule(program, programType, s);
    }
  }

  @Override
  public List<ScheduledRuntime> nextScheduledRuntime(Id.Program program, SchedulableProgramType programType)
    throws SchedulerException {
    return ImmutableList.of();
  }

  @Override
  public List<String> getScheduleIds(Id.Program program, SchedulableProgramType programType)
    throws SchedulerException {
    char startChar = ':';
    char endChar = (char) (startChar + 1);
    String programScheduleId = AbstractSchedulerService.programIdFor(program, programType);
    return ImmutableList.copyOf(scheduleSubscribers.subMap(String.format("%s%c", programScheduleId, startChar),
                                                           String.format("%s%c", programScheduleId, endChar)).keySet());
  }

  @Override
  public void suspendSchedule(Id.Program program, SchedulableProgramType programType, String scheduleName)
    throws ScheduleNotFoundException, SchedulerException {
    String scheduleId = AbstractSchedulerService.scheduleIdFor(program, programType, scheduleName);
    StreamSubscriber subscriber = scheduleSubscribers.get(scheduleId);
    if (subscriber == null) {
      throw new ScheduleNotFoundException(scheduleName);
    }
    subscriber.suspendScheduleTask(program, programType, scheduleName);
  }

  @Override
  public void resumeSchedule(Id.Program program, SchedulableProgramType programType, String scheduleName)
    throws ScheduleNotFoundException, SchedulerException {
    String scheduleId = AbstractSchedulerService.scheduleIdFor(program, programType, scheduleName);
    StreamSubscriber subscriber = scheduleSubscribers.get(scheduleId);
    if (subscriber == null) {
      throw new ScheduleNotFoundException(scheduleName);
    }
    subscriber.resumeScheduleTask(program, programType, scheduleName);
  }

  @Override
  public void updateSchedule(Id.Program program, SchedulableProgramType programType, Schedule schedule)
    throws NotFoundException, SchedulerException {
    Preconditions.checkArgument(schedule instanceof StreamSizeSchedule,
                                "Schedule should be of type StreamSizeSchedule");
    StreamSizeSchedule streamSizeSchedule = (StreamSizeSchedule) schedule;
    StreamSubscriber subscriber = scheduleSubscribers.get(AbstractSchedulerService.scheduleIdFor(program, programType,
                                                                                                 schedule.getName()));
    if (subscriber == null) {
      throw new ScheduleNotFoundException(schedule.getName());
    }

    if (!streamSizeSchedule.getStreamName().equals(subscriber.getStreamId().getName())) {
      // For a change of stream, it's okay to delete the schedule and recreate it

      // It can happen that the schedule was deleted while being updated. In which case, we propagate the
      // not found exception
      deleteSchedule(program, programType, schedule.getName());

      schedule(program, programType, schedule);
    } else {
      // The subscriber will take care of updating the data trigger
      subscriber.updateScheduleTask(program, programType, streamSizeSchedule);
    }
  }

  @Override
  public void deleteSchedule(Id.Program programId, SchedulableProgramType programType, String scheduleName)
    throws ScheduleNotFoundException, SchedulerException {
    String scheduleId = AbstractSchedulerService.scheduleIdFor(programId, programType, scheduleName);
    StreamSubscriber subscriber = scheduleSubscribers.remove(scheduleId);
    if (subscriber == null) {
      throw new ScheduleNotFoundException(scheduleName);
    }
    subscriber.deleteSchedule(programId, programType, scheduleName);
    // We don't delete a StreamSubscriber, even if it has zero task. We keep an empty subscriber so that we don't
    // have to worry about race conditions between add/delete of schedules
  }

  @Override
  public void deleteSchedules(Id.Program programId, SchedulableProgramType programType) throws SchedulerException {
    char startChar = ':';
    char endChar = (char) (startChar + 1);
    String programScheduleId = AbstractSchedulerService.programIdFor(programId, programType);
    NavigableSet<String> scheduleIds = scheduleSubscribers.subMap(String.format("%s%c", programScheduleId, startChar),
                                                                  String.format("%s%c", programScheduleId, endChar))
      .keySet();
    int scheduleIdIdx = programScheduleId.length() + 1;
    for (String scheduleId : scheduleIds) {
      try {
        if (scheduleId.length() < scheduleIdIdx) {
          LOG.warn("Format of scheduleID incorrect: {}", scheduleId);
          continue;
        }
        deleteSchedule(programId, programType, scheduleId.substring(scheduleIdIdx));
      } catch (ScheduleNotFoundException e) {
        // Could be a race, the schedule has just been deleted
        LOG.debug("Could not delete schedule, it might have been deleted already by another thread '{}'",
                  scheduleId, e);
      }
    }
  }

  @Override
  public ScheduleState scheduleState(Id.Program program, SchedulableProgramType programType, String scheduleName)
    throws SchedulerException {
    StreamSubscriber subscriber = scheduleSubscribers.get(AbstractSchedulerService.scheduleIdFor(program, programType,
                                                                                                 scheduleName));
    if (subscriber != null) {
      return subscriber.scheduleTaskState(program, programType, scheduleName);
    } else {
      return ScheduleState.NOT_FOUND;
    }
  }

  private static long toBytes(int mb) {
    return ((long) mb) * 1024 * 1024;
  }

  /**
   * One instance of this class contains a list of {@link StreamSizeSchedule}s, which are all interested
   * in the same stream. This instance subscribes to the size notification of the stream, and polls the
   * stream for its size whenever the schedules it references need the information.
   * The {@link StreamSizeScheduler} communicates with this class, which in turn communicates to the schedules
   * it contains to perform operations on the schedules - suspend, resume, etc.
   */
  private final class StreamSubscriber extends AbstractScheduledService
    implements NotificationHandler<StreamSizeNotification> {
    // Key is the schedule ID
    private final ConcurrentMap<String, StreamSizeScheduleTask> scheduleTasks;
    private final Id.Stream streamId;
    private final AtomicInteger activeTasks;
    private final Object deltaLock;

    private Cancellable notificationSubscription;
    private StreamSizeNotification lastNotification;
    private StreamSize lastPollingInfo;

    // delta represents the gap between notifications for the stream and the stream size metric
    private Long delta;

    private StreamSubscriber(Id.Stream streamId) {
      this.streamId = streamId;
      this.scheduleTasks = Maps.newConcurrentMap();
      this.activeTasks = new AtomicInteger(0);
      this.delta = null;
      this.deltaLock = new Object();
    }

    @Override
    protected void startUp() throws Exception {
      notificationSubscription = notificationService.subscribe(getFeed(), this, sendPollingInfoExecutor);
    }

    @Override
    protected void shutDown() throws Exception {
      if (notificationSubscription != null) {
        notificationSubscription.cancel();
      }
    }

    @Override
    protected void runOneIteration() throws Exception {
      if (activeTasks.get() == 0) {
        return;
      }

      try {
        StreamSize streamSize = pollOnce();
        sendPollingInfoToActiveTasks(streamSize);
      } catch (IOException e) {
        LOG.error("Could not poll stream {}", streamId.getName(), e);
      } catch (Throwable t) {
        LOG.error("Error in scheduled polling for stream {}", streamId.getName(), t);
      }
    }

    @Override
    protected Scheduler scheduler() {
      return Scheduler.newFixedRateSchedule(pollingDelay, pollingDelay, TimeUnit.MILLISECONDS);
    }

    @Override
    protected ScheduledExecutorService executor() {
      return streamPollingExecutor;
    }

    @Override
    public Type getNotificationFeedType() {
      return StreamSizeNotification.class;
    }

    @Override
    public void received(final StreamSizeNotification notification, NotificationContext notificationContext) {
      // We only use the stream size notification if it came after the last seen notification
      if (lastNotification != null && notification.getTimestamp() <= lastNotification.getTimestamp()) {
        return;
      }

      lastNotification = notification;
      if (activeTasks.get() <= 0) {
        return;
      }

      boolean poll = false;
      Long estimate = null;
      synchronized (deltaLock) {
        if (delta == null) {
          poll = true;
        } else {
          for (StreamSizeScheduleTask streamSizeScheduleTask : scheduleTasks.values()) {
            if (streamSizeScheduleTask.shouldTriggerProgram(notification.getSize() - delta)) {
              poll = true;
              estimate = notification.getSize() - delta;
              break;
            }
          }
        }
      }

      if (poll) {
        pollAfterNotification(notification, estimate);
      }
    }

    /**
     * Poll the stream size using metrics after receiving a notification, either to set the delta between metric value
     * and notification value, or because the notification indicates that one {@link StreamSizeScheduleTask} will
     * execute, and polling the stream is required to confirm the information.
     *
     * @param notification {@link StreamSizeNotification} received which triggered polling
     * @param estimate size of data present in the stream, in bytes, which will trigger the execution of a program
     *                 in one of the {@link StreamSizeScheduleTask} present in this {@link StreamSubscriber}. It
     *                 can be null if, when receiving the {@code notification}, not enough information was present
     *                 to compute an estimate - ie, the {@code delta} was null
     */
    private void pollAfterNotification(final StreamSizeNotification notification, @Nullable final Long estimate) {
      final AtomicBoolean firstPoll = new AtomicBoolean(true);
      final AtomicInteger pollRetry = new AtomicInteger(POLLING_AFTER_NOTIFICATION_RETRY);
      pollBookingExecutor.schedule(new Runnable() {
        @Override
        public void run() {
          try {
            StreamSize streamSize;
            boolean estimateReached = false;
            synchronized (deltaLock) {
              streamSize = pollOnce();
              if (firstPoll.compareAndSet(true, false)) {
                // The first polling will recalibrate the delta, for future use when receiving a notification
                delta = notification.getSize() - streamSize.getSize();
              }
              if (estimate != null && streamSize.getSize() >= estimate) {
                estimateReached = true;
              }
            }

            // It is always worth it to send the latest stream size to the active tasks. Even if the estimate was
            // not reached, one of the tasks might expect less data than shown by the estimate
            sendPollingInfoToActiveTasks(streamSize);

            // TODO instead of relying on expected size to retry polling, use notification timestamp VS
            // metric timestamp [CDAP-1676]
            if (estimate != null && !estimateReached && pollRetry.decrementAndGet() >= 0) {
              pollBookingExecutor.schedule(this, Constants.MetricsCollector.DEFAULT_FREQUENCY_SECONDS, TimeUnit.SECONDS);
            } else if (estimate != null && !estimateReached) {
              LOG.debug("Polling estimate {} was not reached for stream {} after {} retries", estimate, streamId.getName(), POLLING_AFTER_NOTIFICATION_RETRY);
            }
          } catch (IOException e) {
            LOG.error("Could not poll stream {}", streamId.getName(), e);
          } catch (Throwable t) {
            LOG.error("Error when polling stream {} and sending info to active tasks", streamId.getName(), t);
          }
        }
      }, Constants.MetricsCollector.DEFAULT_FREQUENCY_SECONDS, TimeUnit.SECONDS);
    }

    /**
     * Add a new scheduling task to this {@link StreamSubscriber}.
     *
     * @param programId Program that needs to be run
     * @param programType type of program
     * @param streamSizeSchedule Schedule with which the program runs
     * @param active {@code true} if this schedule is active, {@code false} otherwise
     * @param basePollSize size, in bytes, used as the base count for this schedule, or -1 to start counting from
     *                     the current size of events ingested by the stream, as indicated by metrics. Another way
     *                     to see it is: size of the stream during which the schedule last executed the program,
     *                     or -1 if it never happened yet
     * @param basePollTs timestamp, in milliseconds, which matches the time at which {@code basePollSize} was computed.
     *                   -1 indicates to start counting from the current timestamp
     * @param lastRunSize size, in bytes, seen during the last execution of the {@code program}, or -1 if it never
     *                    happened yet
     * @param lastRunTs timestamp, in milliseconds, at which the {@code lastRunSize} was computed. -1 indicates that
     *                  the {@code program} was never executed via this {@code streamSizeSchedule} before
     * @param persist {@code true} if this schedule should be persisted in the persistent store containing the
     *                stream size schedules, {@code false} otherwise
     */
    public void createScheduleTask(Id.Program programId, SchedulableProgramType programType,
                                   StreamSizeSchedule streamSizeSchedule, boolean active,
                                   long basePollSize, long basePollTs, long lastRunSize, long lastRunTs,
                                   boolean persist) throws SchedulerException {
      StreamSizeScheduleTask newTask = new StreamSizeScheduleTask(programId, programType, streamSizeSchedule);
      StreamSize streamSize;
      synchronized (this) {
        StreamSizeScheduleTask previous = scheduleTasks.putIfAbsent(
          AbstractSchedulerService.scheduleIdFor(programId, programType, streamSizeSchedule.getName()), newTask);
        if (previous != null) {
          // We cannot replace an existing schedule - that functionality is not wanted - yet
          throw new SchedulerException("Tried to overwrite schedule " + streamSizeSchedule.getName());
        }

        try {
          streamSize = pollOnce();
        } catch (IOException e) {
          // Polling should not fail - the whole logic of stream size schedules relies on it
          throw new SchedulerException("Polling could not be performed on stream " + streamSizeSchedule.getStreamName(),
                                       e);
        }

        // Initialize the schedule task
        if (basePollSize == -1 && basePollTs == -1) {
          newTask.startSchedule(streamSize.getSize(), streamSize.getTimestamp(), lastRunSize, lastRunTs,
                                active, persist);
        } else {
          newTask.startSchedule(basePollSize, basePollTs, lastRunSize, lastRunTs, active, persist);
        }

        if (active) {
          activeTasks.incrementAndGet();
        }
      }

      sendPollingInfoToActiveTasks(streamSize);
    }

    /**
     * Suspend a scheduling task that is based on the data received by the stream referenced by {@code this} object.
     */
    public synchronized void suspendScheduleTask(Id.Program programId, SchedulableProgramType programType,
                                                 String scheduleName) throws ScheduleNotFoundException {
      String scheduleId = AbstractSchedulerService.scheduleIdFor(programId, programType, scheduleName);
      StreamSizeScheduleTask task = scheduleTasks.get(scheduleId);
      if (task == null) {
        throw new ScheduleNotFoundException(scheduleName);
      }
      if (task.suspend()) {
        activeTasks.decrementAndGet();
      }
    }

    /**
     * Resume a scheduling task that is based on the data received by the stream referenced by {@code this} object.
     */
    public synchronized void resumeScheduleTask(Id.Program programId, SchedulableProgramType programType,
                                                String scheduleName) throws ScheduleNotFoundException {
      final StreamSizeScheduleTask task;
      String scheduleId = AbstractSchedulerService.scheduleIdFor(programId, programType, scheduleName);
      task = scheduleTasks.get(scheduleId);
      if (task == null) {
        throw new ScheduleNotFoundException(scheduleName);
      }
      if (!task.resume()) {
        return;
      }

      activeTasks.incrementAndGet();

      try {
        StreamSize streamSize = pollOnce();
        sendPollingInfoToActiveTasks(streamSize);
      } catch (IOException e) {
        LOG.debug("Ignoring stream events size polling after resuming schedule {} due to error",
                  scheduleName, e);
      }
    }

    /**
     * Updates the task of this {@link StreamSubscriber} that has the same ID as the given {@code schedule}
     * with the new schedule.
     */
    public synchronized void updateScheduleTask(Id.Program program, SchedulableProgramType programType,
                                                StreamSizeSchedule schedule)
      throws ScheduleNotFoundException {
      String scheduleId = AbstractSchedulerService.scheduleIdFor(program, programType, schedule.getName());
      final StreamSizeScheduleTask scheduleTask = scheduleTasks.get(scheduleId);
      if (scheduleTask == null) {
        throw new ScheduleNotFoundException(schedule.getName());
      }
      scheduleTask.updateSchedule(schedule);

      try {
        StreamSize streamSize = pollOnce();
        sendPollingInfoToActiveTasks(streamSize);
      } catch (IOException e) {
        LOG.debug("Ignoring stream events size polling after resuming schedule {} due to error",
                  schedule.getName(), e);
      }
    }

    /**
     * Delete a scheduling task that is based on the data received by the stream referenced by {@code this} object.
     */
    public synchronized void deleteSchedule(Id.Program programId, SchedulableProgramType programType,
                                            String scheduleName) throws ScheduleNotFoundException {
      String scheduleId = AbstractSchedulerService.scheduleIdFor(programId, programType, scheduleName);
      StreamSizeScheduleTask scheduleTask = scheduleTasks.remove(scheduleId);
      if (scheduleTask == null) {
        throw new ScheduleNotFoundException(scheduleName);
      }
      if (scheduleTask.isActive()) {
        activeTasks.decrementAndGet();
      }
    }

    /**
     * Get the status a scheduling task that is based on the data received by the stream referenced by {@code this}
     * object.
     */
    public ScheduleState scheduleTaskState(Id.Program programId, SchedulableProgramType programType,
                                           String scheduleName) {
      StreamSizeScheduleTask task = scheduleTasks.get(AbstractSchedulerService.scheduleIdFor(programId, programType,
                                                                                             scheduleName));
      if (task == null) {
        return ScheduleState.NOT_FOUND;
      }
      return task.isActive() ? ScheduleState.SCHEDULED : ScheduleState.SUSPENDED;
    }

    public Id.Stream getStreamId() {
      return streamId;
    }

    /**
     * Poll the stream size using metrics.
     */
    private synchronized StreamSize pollOnce() throws IOException {
      StreamSize streamSize = queryStreamEventsSize();
      if (lastPollingInfo != null && streamSize.getSize() < lastPollingInfo.getSize()) {
        delta = null;
        for (StreamSizeScheduleTask streamSizeScheduleTask : scheduleTasks.values()) {
          streamSizeScheduleTask.reset(streamSize.getTimestamp());
        }
      }
      lastPollingInfo = streamSize;
      return streamSize;
    }

    /**
     * Send a {@link StreamSize} built using information from the stream metrics to all the active
     * {@link StreamSizeSchedule} referenced by this object.
     */
    private void sendPollingInfoToActiveTasks(final StreamSize pollingInfo) {
      for (final StreamSizeScheduleTask task : scheduleTasks.values()) {
        if (!task.isActive()) {
          continue;
        }
        sendPollingInfoExecutor.execute(new Runnable() {
          @Override
          public void run() {
            task.receivedPollingInformation(pollingInfo);
          }
        });
      }
    }

    private Id.NotificationFeed getFeed() {
      return new Id.NotificationFeed.Builder()
        .setNamespaceId(streamId.getNamespaceId())
        .setCategory(Constants.Notification.Stream.STREAM_FEED_CATEGORY)
        .setName(String.format("%sSize", streamId.getName()))
        .build();
    }

    /**
     * Query the metrics system to get the size of events ingested by a stream.
     *
     * @return size of events ingested by the stream so far, queried using the metric system
     */
    private StreamSize queryStreamEventsSize() throws IOException {
      MetricDataQuery metricDataQuery = new MetricDataQuery(
        0L, TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()),
        Integer.MAX_VALUE, "system.collect.bytes",
        MetricType.COUNTER,
        ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, streamId.getNamespaceId(),
                        Constants.Metrics.Tag.STREAM, streamId.getName()),
        ImmutableList.<String>of()
      );

      try {
        Collection<MetricTimeSeries> metrics = metricStore.query(metricDataQuery);
        if (metrics == null || metrics.isEmpty()) {
          // Data is not yet available, which means no data has been ingested by the stream yet
          return new StreamSize(0L, System.currentTimeMillis());
        }

        MetricTimeSeries metric = metrics.iterator().next();
        List<TimeValue> timeValues = metric.getTimeValues();
        if (timeValues == null || timeValues.size() != 1) {
          throw new IOException("Should collect exactly one time value");
        }
        TimeValue timeValue = timeValues.get(0);
        // The metric store gives us 0 as the timestamp, hence we cannot use it here
        return new StreamSize(timeValue.getValue(), System.currentTimeMillis());
      } catch (Exception e) {
        Throwables.propagateIfInstanceOf(e, IOException.class);
        throw new IOException(e);
      }
    }
  }

  /**
   * Wrapper around a {@link StreamSizeSchedule} which will run a program whenever it receives enough
   * data from a stream, via notifications.
   */
  private final class StreamSizeScheduleTask {
    private final Id.Program programId;
    private final SchedulableProgramType programType;
    private final AtomicBoolean active;
    private StreamSizeSchedule streamSizeSchedule;

    // Size, in bytes, given by the polling info which serves as a base when comparing polling info
    private long basePollSize;
    // Time, in milliseconds, when the previous attribute was computed
    private long basePollTs;

    // Size, in bytes, of the stream at the last recorded execution
    private long lastRunSize;
    // Logical time, in milliseconds, of the last recorded execution
    private long lastRunTs;

    private StreamSizeScheduleTask(Id.Program programId, SchedulableProgramType programType,
                                   StreamSizeSchedule streamSizeSchedule) {
      this.programId = programId;
      this.programType = programType;
      this.streamSizeSchedule = streamSizeSchedule;
      this.active = new AtomicBoolean(false);
    }

    /**
     * Start a stream size schedule task.
     *
     * @param basePollSize base size of the stream to start counting from. This info comes from polling the stream
     * @param basePollTs time at which the {@code basePollSize} was obtained
     * @param lastRunSize size of the stream during the last run of the program. -1 if there is no such run
     * @param lastRunTs timestamp at which {@code lastRunSize} happened
     * @param active {@code true} if the schedule is active, {@code false} if it is suspended
     * @param persist {@code true} if this information should be persisted, {@code false} otherwise
     */
    public void startSchedule(long basePollSize, long basePollTs, long lastRunSize, long lastRunTs,
                              boolean active, boolean persist) {
      LOG.debug("Starting schedule {} with basePollSize {}, basePollTs {}, active {}. Should be persisted: {}",
                streamSizeSchedule.getName(), basePollSize, basePollTs, active, persist);
      this.basePollSize = basePollSize;
      this.basePollTs = basePollTs;
      this.lastRunSize = lastRunSize;
      this.lastRunTs = lastRunTs;
      this.active.set(active);
    }

    public boolean isActive() {
      return active.get();
    }

    /**
     * Received stream size information coming from polling.
     *
     * @param pollingInfo {@link StreamSize} info that came from polling the stream using metrics
     */
    public void receivedPollingInformation(@Nonnull StreamSize pollingInfo) {
      Preconditions.checkNotNull(pollingInfo);
      if (!active.get()) {
        return;
      }

      StreamSizeSchedule currentSchedule;
      ImmutableMap.Builder<String, String> argsBuilder = ImmutableMap.builder();
      synchronized (this) {
        if (pollingInfo.getSize() - basePollSize < toBytes(streamSizeSchedule.getDataTriggerMB())) {
          return;
        }

        argsBuilder.put(ProgramOptionConstants.SCHEDULE_NAME, streamSizeSchedule.getName());
        argsBuilder.put(ProgramOptionConstants.LOGICAL_START_TIME, Long.toString(pollingInfo.getTimestamp()));
        argsBuilder.put(ProgramOptionConstants.RUN_DATA_SIZE, Long.toString(pollingInfo.getSize()));
        argsBuilder.put(ProgramOptionConstants.RUN_BASE_COUNT_TIME, Long.toString(basePollTs));
        argsBuilder.put(ProgramOptionConstants.RUN_BASE_COUNT_SIZE, Long.toString(basePollSize));

        if (lastRunSize != -1 && lastRunTs != -1) {
          argsBuilder.put(ProgramOptionConstants.LAST_SCHEDULED_RUN_LOGICAL_START_TIME, Long.toString(lastRunTs));
          argsBuilder.put(ProgramOptionConstants.LAST_SCHEDULED_RUN_DATA_SIZE, Long.toString(lastRunSize));
        }

        currentSchedule = streamSizeSchedule;
        basePollSize = pollingInfo.getSize();
        basePollTs = pollingInfo.getTimestamp();
      }

      ScheduleTaskRunner taskRunner = new ScheduleTaskRunner(store, programRuntimeService, preferencesStore);
      try {
        LOG.info("About to start streamSizeSchedule {}", currentSchedule.getName());
        taskRunner.run(programId, ProgramType.valueOf(programType.name()), new BasicArguments(argsBuilder.build()));
        lastRunSize = pollingInfo.getSize();
        lastRunTs = pollingInfo.getTimestamp();
      } catch (TaskExecutionException e) {
        // Note: in case of a failure, we don't reset the base information. We still act as if it was a success
        // and start counting again from the size of that failed run
        LOG.error("Execution exception while running streamSizeSchedule {}", currentSchedule.getName(), e);
      }
    }

    /**
     * @return true if we successfully suspended the schedule, false if it was already suspended
     */
    public boolean suspend() {
      return active.compareAndSet(true, false);
    }

    /**
     * @return true if we successfully resumed the schedule, false if it was already active
     */
    public boolean resume() {
      return active.compareAndSet(false, true);
    }

    /**
     * Replace the {@link StreamSizeSchedule} of this task.
     */
    public synchronized void updateSchedule(StreamSizeSchedule schedule) {
      streamSizeSchedule = schedule;
    }

    /**
     * Indicates whether this task should trigger the execution of the program, or not.
     *
     * @param newEstimateSize estimate of the stream size as of now
     * @return {@code true} if this task should trigger, {@code false} otherwise
     */
    public boolean shouldTriggerProgram(long newEstimateSize) {
      return active.get() && newEstimateSize >= basePollSize + toBytes(streamSizeSchedule.getDataTriggerMB());
    }

    /**
     * Reset the base polling counters - it happens when polling the stream using metrics shows less data than
     * the previous poll. This can only be due to metrics deletion or metric TTL.
     *
     * @param timestamp timestamp set as the new base for polling
     */
    public void reset(long timestamp) {
      basePollSize = 0L;
      basePollTs = timestamp;
    }
  }

  /**
   * Class representing the size of data present in a stream at a given time.
   */
  private final class StreamSize {
    private final long size;
    private final long timestamp;

    private StreamSize(long size, long timestamp) {
      this.size = size;
      this.timestamp = timestamp;
    }

    public long getSize() {
      return size;
    }

    public long getTimestamp() {
      return timestamp;
    }
  }
}

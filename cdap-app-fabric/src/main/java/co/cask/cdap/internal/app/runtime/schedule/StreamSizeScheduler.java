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

import co.cask.cdap.api.dataset.lib.cube.AggregationFunction;
import co.cask.cdap.api.dataset.lib.cube.TimeValue;
import co.cask.cdap.api.metrics.MetricDataQuery;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.api.metrics.MetricTimeSeries;
import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.schedule.Schedule;
import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.stream.notification.StreamSizeNotification;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.schedule.store.DatasetBasedStreamSizeScheduleStore;
import co.cask.cdap.internal.app.services.ProgramLifecycleService;
import co.cask.cdap.internal.app.services.PropertiesResolver;
import co.cask.cdap.internal.schedule.StreamSizeSchedule;
import co.cask.cdap.notifications.service.NotificationContext;
import co.cask.cdap.notifications.service.NotificationHandler;
import co.cask.cdap.notifications.service.NotificationService;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.ScheduledRuntime;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.List;
import java.util.Map;
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
  private final Provider<Store> storeProvider;
  private final ProgramLifecycleService lifecycleService;
  private final PropertiesResolver propertiesResolver;
  private final DatasetBasedStreamSizeScheduleStore scheduleStore;
  private final ConcurrentMap<Id.Stream, StreamSubscriber> streamSubscribers;

  // Key is scheduleId
  private final ConcurrentSkipListMap<String, StreamSubscriber> scheduleSubscribers;

  private Store store;

  private Executor sendPollingInfoExecutor;

  // Used to schedule polling of a stream only after a certain time - the time after which the metrics are updated
  private ScheduledExecutorService pollBookingExecutor;

  // Scheduled executor used to poll stream at regular intervals, by querying the metric system
  private ScheduledExecutorService streamPollingExecutor;

  private ListeningExecutorService taskExecutorService;
  private boolean schedulerStarted;

  @Inject
  public StreamSizeScheduler(CConfiguration cConf, NotificationService notificationService, MetricStore metricStore,
                             Provider<Store> storeProvider, ProgramLifecycleService lifecycleService,
                             PropertiesResolver propertiesResolver, DatasetBasedStreamSizeScheduleStore scheduleStore) {
    this.pollingDelay = TimeUnit.SECONDS.toMillis(
      cConf.getLong(Constants.Notification.Stream.STREAM_SIZE_SCHEDULE_POLLING_DELAY));
    this.notificationService = notificationService;
    this.metricStore = metricStore;
    this.storeProvider = storeProvider;
    this.lifecycleService = lifecycleService;
    this.propertiesResolver = propertiesResolver;
    this.scheduleStore = scheduleStore;
    this.streamSubscribers = Maps.newConcurrentMap();
    this.scheduleSubscribers = new ConcurrentSkipListMap<>();
    this.schedulerStarted = false;
  }

  public void init() throws SchedulerException {
    sendPollingInfoExecutor = Executors.newCachedThreadPool(
      Threads.createDaemonThreadFactory("stream-size-scheduler-%d"));
    streamPollingExecutor = Executors.newScheduledThreadPool(STREAM_POLLING_THREAD_POOL_SIZE,
                                                             Threads.createDaemonThreadFactory("stream-polling-%d"));
    pollBookingExecutor = Executors.newSingleThreadScheduledExecutor(
      Threads.createDaemonThreadFactory("polling-booking-executor"));
    taskExecutorService = MoreExecutors.listeningDecorator(
      Executors.newCachedThreadPool(Threads.createDaemonThreadFactory("stream-schedule-task")));

    store = storeProvider.get();

    initializeScheduleStore();
  }

  void lazyStart() throws SchedulerException {
    schedulerStarted = true;
  }

  boolean isStarted() {
    return schedulerStarted;
  }

  /**
   * Initialize the stream size schedule store, and restart all the schedule tasks that were persisted in it.
   *
   * @throws SchedulerException if the persistent store could not be initialized, or if we couldn't list the
   *                            persisted tasks
   */
  private void initializeScheduleStore() throws SchedulerException {
    try {
      scheduleStore.initialize();
    } catch (Throwable t) {
      throw new SchedulerException("Error when initializing store for stream size schedules", t);
    }

    // Reschedule the persisted schedules
    List<StreamSizeScheduleState> scheduleStates;
    try {
      scheduleStates = scheduleStore.list();
    } catch (Throwable t) {
      throw new SchedulerException("Error when listing stream size schedules from store", t);
    }

    for (StreamSizeScheduleState scheduleState : scheduleStates) {
      try {
        restoreScheduleFromStore(scheduleState.getProgramId(), scheduleState.getProgramType(),
                                 scheduleState.getStreamSizeSchedule(), scheduleState.getProperties(),
                                 scheduleState.isRunning(),
                                 scheduleState.getBaseRunSize(), scheduleState.getBaseRunTs(),
                                 scheduleState.getLastRunSize(), scheduleState.getLastRunTs());
      } catch (SchedulerException e) {
        // We should never enter this, but if we do, we still want the other schedule tasks to be handled
        LOG.error("Could not schedule task '{}' from persistent store", scheduleState, e);
      }
    }

    // Poll all the Streams for active tasks
    for (StreamSubscriber streamSubscriber : streamSubscribers.values()) {
      if (streamSubscriber.getActiveTasksCount() <= 0) {
        continue;
      }

      try {
        StreamSize streamSize = streamSubscriber.pollOnce();
        streamSubscriber.sendPollingInfoToActiveTasks(streamSize);
      } catch (IOException e) {
        // Failing to poll should not make this init fail
        LOG.warn("Could not poll size for stream '{}'", streamSubscriber.getStreamId(), e);
      }
    }
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
    if (taskExecutorService != null) {
      taskExecutorService.shutdownNow();
    }
  }

  @Override
  public void schedule(Id.Program program, SchedulableProgramType programType, Schedule schedule)
    throws SchedulerException {
    schedule(program, programType, schedule, ImmutableMap.<String, String>of());
  }

  @Override
  public void schedule(Id.Program program, SchedulableProgramType programType, Schedule schedule,
                       Map<String, String> properties) throws SchedulerException {
    Preconditions.checkArgument(schedule instanceof StreamSizeSchedule,
                                "Schedule should be of type StreamSizeSchedule");
    StreamSizeSchedule streamSizeSchedule = (StreamSizeSchedule) schedule;
    StreamSubscriber streamSubscriber = streamSubscriberForSchedule(program, streamSizeSchedule);

    // Add the scheduleTask to the StreamSubscriber
    streamSubscriber.createScheduleTask(program, programType, streamSizeSchedule, properties);
    scheduleSubscribers.put(AbstractSchedulerService.scheduleIdFor(program, programType,
                                                                   streamSizeSchedule.getName()),
                            streamSubscriber);
  }

  /**
   * Handle a {@link StreamSizeSchedule} object coming from the persistent store in this scheduler.
   *
   * @param program program that needs to be run
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
   * @throws SchedulerException if the schedule already exists in this {@link StreamSubscriber}
   */
  private void restoreScheduleFromStore(Id.Program program, SchedulableProgramType programType,
                                        StreamSizeSchedule streamSizeSchedule, Map<String, String> properties,
                                        boolean active, long basePollSize,
                                        long basePollTs, long lastRunSize, long lastRunTs) throws SchedulerException {
    StreamSubscriber streamSubscriber = streamSubscriberForSchedule(program, streamSizeSchedule);

    // Add the scheduleTask to the StreamSubscriber
    streamSubscriber.restoreScheduleFromStore(program, programType, streamSizeSchedule, properties, active,
                                              basePollSize, basePollTs, lastRunSize, lastRunTs);
    scheduleSubscribers.put(AbstractSchedulerService.scheduleIdFor(program, programType,
                                                                   streamSizeSchedule.getName()),
                            streamSubscriber);
  }

  @Override
  public void schedule(Id.Program program, SchedulableProgramType programType, Iterable<Schedule> schedules)
    throws SchedulerException {
    schedule(program, programType, schedules, ImmutableMap.<String, String>of());
  }

  @Override
  public void schedule(Id.Program program, SchedulableProgramType programType, Iterable<Schedule> schedules,
                       Map<String, String> properties) throws SchedulerException {
    for (Schedule s : schedules) {
      schedule(program, programType, s, properties);
    }
  }

  @Override
  public List<ScheduledRuntime> previousScheduledRuntime(Id.Program program, SchedulableProgramType programType)
    throws SchedulerException {
    return ImmutableList.of();
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
      throw new ScheduleNotFoundException(Id.Schedule.from(program.getApplication(), scheduleName));
    }
    subscriber.suspendScheduleTask(program, programType, scheduleName);
  }

  @Override
  public void resumeSchedule(Id.Program program, SchedulableProgramType programType, String scheduleName)
    throws ScheduleNotFoundException, SchedulerException {
    String scheduleId = AbstractSchedulerService.scheduleIdFor(program, programType, scheduleName);
    StreamSubscriber subscriber = scheduleSubscribers.get(scheduleId);
    if (subscriber == null) {
      throw new ScheduleNotFoundException(Id.Schedule.from(program.getApplication(), scheduleName));
    }
    subscriber.resumeScheduleTask(program, programType, scheduleName);
  }

  @Override
  public void updateSchedule(Id.Program program, SchedulableProgramType programType, Schedule schedule)
    throws NotFoundException, SchedulerException {
    updateSchedule(program, programType, schedule, ImmutableMap.<String, String>of());
  }

  @Override
  public void updateSchedule(Id.Program program, SchedulableProgramType programType, Schedule schedule,
                             Map<String, String> properties) throws NotFoundException, SchedulerException {
    Preconditions.checkArgument(schedule instanceof StreamSizeSchedule,
                                "Schedule should be of type StreamSizeSchedule");
    StreamSizeSchedule streamSizeSchedule = (StreamSizeSchedule) schedule;
    StreamSubscriber subscriber = scheduleSubscribers.get(AbstractSchedulerService.scheduleIdFor(program, programType,
                                                                                                 schedule.getName()));
    if (subscriber == null) {
      throw new ScheduleNotFoundException(Id.Schedule.from(program.getApplication(), schedule.getName()));
    }

    if (!streamSizeSchedule.getStreamName().equals(subscriber.getStreamId().getId())) {
      // For a change of stream, it's okay to delete the schedule and recreate it

      // It can happen that the schedule was deleted while being updated. In which case, we propagate the
      // not found exception
      deleteSchedule(program, programType, schedule.getName());

      schedule(program, programType, schedule, properties);
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
      throw new ScheduleNotFoundException(Id.Schedule.from(programId.getApplication(), scheduleName));
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
  public void deleteAllSchedules(Id.Namespace namespaceId) throws SchedulerException {
    for (ApplicationSpecification appSpec : store.getAllApplications(namespaceId)) {
      deleteAllSchedules(namespaceId, appSpec);
    }
  }

  private void deleteAllSchedules(Id.Namespace namespaceId, ApplicationSpecification appSpec)
    throws SchedulerException {
    for (ScheduleSpecification scheduleSpec : appSpec.getSchedules().values()) {
      Id.Application appId = Id.Application.from(namespaceId.getId(), appSpec.getName());
      ProgramType programType = ProgramType.valueOfSchedulableType(scheduleSpec.getProgram().getProgramType());
      Id.Program programId = Id.Program.from(appId, programType, scheduleSpec.getProgram().getProgramName());
      deleteSchedules(programId, scheduleSpec.getProgram().getProgramType());
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

  /**
   * Create or retrieve the {@link StreamSubscriber} object corresponding to the Stream contained in the
   * {@code streamSizeSchedule}.
   *
   * @param program program that needs to be run - containing the namespace ID of the Stream
   * @param streamSizeSchedule schedule object containing the Stream name for which a subscriber needs to be
   *                           created/retrieved
   * @return {@link StreamSubscriber} object corresponding to the Stream contained in the
   *         {@code streamSizeSchedule}
   */
  private StreamSubscriber streamSubscriberForSchedule(Id.Program program, StreamSizeSchedule streamSizeSchedule) {
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
    return streamSubscriber;
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
        LOG.error("Could not poll stream {}", streamId.getId(), e);
      } catch (Throwable t) {
        LOG.error("Error in scheduled polling for stream {}", streamId.getId(), t);
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
    public Type getNotificationType() {
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
     * @return number of active schedule tasks for this {@link StreamSubscriber}
     */
    public int getActiveTasksCount() {
      return activeTasks.get();
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
              pollBookingExecutor.schedule(this, Constants.MetricsCollector.DEFAULT_FREQUENCY_SECONDS,
                                           TimeUnit.SECONDS);
            } else if (estimate != null && !estimateReached) {
              LOG.debug("Polling estimate {} was not reached for stream {} after {} retries",
                        estimate, streamId.getId(), POLLING_AFTER_NOTIFICATION_RETRY);
            }
          } catch (IOException e) {
            LOG.error("Could not poll stream {}", streamId.getId(), e);
          } catch (Throwable t) {
            LOG.error("Error when polling stream {} and sending info to active tasks", streamId.getId(), t);
          }
        }
      }, Constants.MetricsCollector.DEFAULT_FREQUENCY_SECONDS, TimeUnit.SECONDS);
    }

    /**
     * Add a new scheduling task to this {@link StreamSubscriber}.
     */
    public void createScheduleTask(Id.Program programId, SchedulableProgramType programType,
                                   StreamSizeSchedule streamSizeSchedule, Map<String, String> properties)
      throws SchedulerException {
      StreamSize streamSize;
      synchronized (this) {
        String scheduleId = AbstractSchedulerService.scheduleIdFor(programId, programType,
                                                                   streamSizeSchedule.getName());
        StreamSizeScheduleTask previous = scheduleTasks.get(scheduleId);
        if (previous != null) {
          // We cannot replace an existing schedule - that functionality is not wanted - yet
          throw new SchedulerException("Tried to overwrite schedule " + streamSizeSchedule.getName());
        }

        try {
          streamSize = pollOnce();
        } catch (IOException e) {
          // Polling should not fail when creating a schedule for the first time -
          // the whole logic of stream size schedules relies on it
          throw new SchedulerException("Polling could not be performed on stream " +
                                         streamSizeSchedule.getStreamName(),
                                       e);
        }

        // Initialize the schedule task
        StreamSizeScheduleTask newTask = new StreamSizeScheduleTask(programId, programType, streamSizeSchedule,
                                                                    properties);

        // First time that we create this schedule, it has to be initialized with the latest polling info
        newTask.startNewSchedule(streamSize.getSize(), streamSize.getTimestamp());

        // We only modify the scheduleTasks if the persistence in startSchedule() did not throw any exception
        scheduleTasks.put(scheduleId, newTask);

        activeTasks.incrementAndGet();
      }

      sendPollingInfoToActiveTasks(streamSize);
    }

    /**
     * Add scheduling task coming from the persistent store to this {@link StreamSubscriber}.
     *
     * @param programId Program that needs to be run
     * @param programType type of program
     * @param streamSizeSchedule Schedule with which the program runs
     * @param properties properties to be passed to the program to be started
     * @param active {@code true} if this schedule is active, {@code false} otherwise
     * @param basePollSize size, in bytes, used as the base count for this schedule
     * @param basePollTs timestamp, in milliseconds, which matches the time at which {@code basePollSize} was computed
     * @param lastRunSize size, in bytes, seen during the last execution of the {@code program}, or -1 if it never
     *                    happened yet
     * @param lastRunTs timestamp, in milliseconds, at which the {@code lastRunSize} was computed. -1 indicates that
     *                  the {@code program} was never executed via this {@code streamSizeSchedule} before
     * @throws SchedulerException if the schedule already exists in this {@link StreamSubscriber}
     */
    public synchronized void restoreScheduleFromStore(Id.Program programId, SchedulableProgramType programType,
                                                      StreamSizeSchedule streamSizeSchedule,
                                                      Map<String, String> properties, boolean active,
                                                      long basePollSize, long basePollTs, long lastRunSize,
                                                      long lastRunTs) throws SchedulerException {
      String scheduleId = AbstractSchedulerService.scheduleIdFor(programId, programType,
                                                                 streamSizeSchedule.getName());
      StreamSizeScheduleTask previous = scheduleTasks.get(scheduleId);
      if (previous != null) {
        // We cannot replace an existing schedule - that functionality is not wanted - yet
        throw new SchedulerException("Tried to overwrite schedule " + streamSizeSchedule.getName());
      }

      // Initialize the schedule task
      StreamSizeScheduleTask newTask = new StreamSizeScheduleTask(programId, programType, streamSizeSchedule,
                                                                  properties);
      newTask.startScheduleFromStore(basePollSize, basePollTs, lastRunSize, lastRunTs, active);
      scheduleTasks.put(scheduleId, newTask);

      if (active) {
        activeTasks.incrementAndGet();
      }
    }

    /**
     * Suspend a scheduling task that is based on the data received by the stream referenced by {@code this} object.
     */
    public synchronized void suspendScheduleTask(Id.Program programId, SchedulableProgramType programType,
                                                 String scheduleName)
      throws ScheduleNotFoundException, SchedulerException {

      String scheduleId = AbstractSchedulerService.scheduleIdFor(programId, programType, scheduleName);
      StreamSizeScheduleTask task = scheduleTasks.get(scheduleId);
      if (task == null) {
        throw new ScheduleNotFoundException(Id.Schedule.from(programId.getApplication(), scheduleName));
      }
      if (task.suspend()) {
        activeTasks.decrementAndGet();
      }
    }

    /**
     * Resume a scheduling task that is based on the data received by the stream referenced by {@code this} object.
     */
    public synchronized void resumeScheduleTask(Id.Program programId, SchedulableProgramType programType,
                                                String scheduleName)
      throws ScheduleNotFoundException, SchedulerException {
      final StreamSizeScheduleTask task;
      String scheduleId = AbstractSchedulerService.scheduleIdFor(programId, programType, scheduleName);
      task = scheduleTasks.get(scheduleId);
      if (task == null) {
        throw new ScheduleNotFoundException(Id.Schedule.from(programId.getApplication(), scheduleName));
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
      throws ScheduleNotFoundException, SchedulerException {

      Id.Schedule scheduleId = Id.Schedule.from(program.getApplication(), schedule.getName());
      String scheduleIdString = AbstractSchedulerService.scheduleIdFor(program, programType, schedule.getName());
      final StreamSizeScheduleTask scheduleTask = scheduleTasks.get(scheduleIdString);
      if (scheduleTask == null) {
        throw new ScheduleNotFoundException(scheduleId);
      }
      scheduleTask.updateSchedule(schedule);

      try {
        StreamSize streamSize = pollOnce();
        sendPollingInfoToActiveTasks(streamSize);
      } catch (IOException e) {
        LOG.debug("Ignoring stream events size polling after resuming schedule {} due to error",
                  scheduleId.getId(), e);
      }
    }

    /**
     * Delete a scheduling task that is based on the data received by the stream referenced by {@code this} object.
     */
    public synchronized void deleteSchedule(Id.Program programId, SchedulableProgramType programType,
                                            String scheduleName) throws ScheduleNotFoundException, SchedulerException {

      String scheduleIdString = AbstractSchedulerService.scheduleIdFor(programId, programType, scheduleName);
      Id.Schedule scheduleId = Id.Schedule.from(programId.getApplication(), scheduleName);
      StreamSizeScheduleTask scheduleTask = scheduleTasks.get(scheduleIdString);
      if (scheduleTask == null) {
        throw new ScheduleNotFoundException(scheduleId);
      }
      scheduleTask.deleteFromStore();

      // The task is only removed from the map of scheduleTasks if deleting it from the persistent
      // store succeeded in the previous call
      scheduleTasks.remove(scheduleIdString);

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

      String scheduleIdString = AbstractSchedulerService.scheduleIdFor(programId, programType, scheduleName);
      StreamSizeScheduleTask task = scheduleTasks.get(scheduleIdString);
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
        .setName(String.format("%sSize", streamId.getId()))
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
        AggregationFunction.SUM,
        ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, streamId.getNamespaceId(),
                        Constants.Metrics.Tag.STREAM, streamId.getId()),
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
    private final Map<String, String> properties;
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
                                   StreamSizeSchedule streamSizeSchedule, Map<String, String> properties) {
      this.programId = programId;
      this.programType = programType;
      this.streamSizeSchedule = streamSizeSchedule;
      this.properties = (properties == null) ? ImmutableMap.<String, String>of() : ImmutableMap.copyOf(properties);
      this.active = new AtomicBoolean(false);
    }

    /**
     * Start a new stream size schedule task. The task is set as active, and its last run information
     * set to -1.
     *
     * @param basePollSize base size of the stream to start counting from. This info comes from polling the stream
     * @param basePollTs time at which the {@code basePollSize} was obtained
     */
    public void startNewSchedule(long basePollSize, long basePollTs) throws SchedulerException {
      LOG.debug("Starting new schedule {} with basePollSize {}, basePollTs {}",
                streamSizeSchedule.getName(), basePollSize, basePollTs);
      this.basePollSize = basePollSize;
      this.basePollTs = basePollTs;
      this.lastRunSize = -1;
      this.lastRunTs = -1;
      this.active.set(true);

      try {
        scheduleStore.persist(programId, programType, streamSizeSchedule, properties,
                              basePollSize, basePollTs, lastRunSize,
                              lastRunTs, this.active.get());
      } catch (Throwable t) {
        throw new SchedulerException("Error when persisting schedule " + streamSizeSchedule.getName() + "in store",
                                     t);
      }
    }

    /**
     * Start a stream size schedule task that comes from the persistent store.
     *
     * @param basePollSize base size of the stream to start counting from. This info comes from polling the stream
     * @param basePollTs time at which the {@code basePollSize} was obtained
     * @param lastRunSize size of the stream during the last run of the program. -1 if there is no such run
     * @param lastRunTs timestamp at which {@code lastRunSize} happened
     * @param active {@code true} if the schedule is active, {@code false} if it is suspended
     */
    public void startScheduleFromStore(long basePollSize, long basePollTs, long lastRunSize, long lastRunTs,
                                       boolean active) {
      LOG.debug("Starting schedule from store {} with basePollSize {}, basePollTs {}, active {}",
                streamSizeSchedule.getName(), basePollSize, basePollTs, active);
      this.basePollSize = basePollSize;
      this.basePollTs = basePollTs;
      this.lastRunSize = lastRunSize;
      this.lastRunTs = lastRunTs;
      this.active.set(active);
    }

    /**
     * Remove the schedule task information from the persistent store.
     *
     * @throws SchedulerException when the task could not be deleted from the store
     */
    public void deleteFromStore() throws SchedulerException {
      try {
        scheduleStore.delete(programId, programType, streamSizeSchedule.getName());
      } catch (Throwable t) {
        throw new SchedulerException("Error when deleting schedule " + streamSizeSchedule.getName() + "from store",
                                     t);
      }
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

      final StreamSizeSchedule currentSchedule;
      final ImmutableMap.Builder<String, String> argsBuilder = ImmutableMap.builder();
      synchronized (this) {
        if (pollingInfo.getSize() - basePollSize < toBytes(streamSizeSchedule.getDataTriggerMB())) {
          return;
        }

        argsBuilder.put(ProgramOptionConstants.SCHEDULE_NAME, streamSizeSchedule.getName());
        argsBuilder.put(ProgramOptionConstants.LOGICAL_START_TIME, Long.toString(pollingInfo.getTimestamp()));
        argsBuilder.put(ProgramOptionConstants.RUN_DATA_SIZE, Long.toString(pollingInfo.getSize()));
        argsBuilder.put(ProgramOptionConstants.RUN_BASE_COUNT_TIME, Long.toString(basePollTs));
        argsBuilder.put(ProgramOptionConstants.RUN_BASE_COUNT_SIZE, Long.toString(basePollSize));
        argsBuilder.putAll(properties);

        if (lastRunSize != -1 && lastRunTs != -1) {
          argsBuilder.put(ProgramOptionConstants.LAST_SCHEDULED_RUN_LOGICAL_START_TIME, Long.toString(lastRunTs));
          argsBuilder.put(ProgramOptionConstants.LAST_SCHEDULED_RUN_DATA_SIZE, Long.toString(lastRunSize));
        }

        try {
          scheduleStore.updateBaseRun(programId, programType, streamSizeSchedule.getName(),
                                      pollingInfo.getSize(), pollingInfo.getTimestamp());
        } catch (Throwable t) {
          LOG.error("Error when persisting new base information for schedule {} in store. Program will not be executed",
                    streamSizeSchedule.getName(), t);
          return;
        }

        currentSchedule = streamSizeSchedule;
        basePollSize = pollingInfo.getSize();
        basePollTs = pollingInfo.getTimestamp();
      }

      final ScheduleTaskRunner taskRunner = new ScheduleTaskRunner(store, lifecycleService, propertiesResolver,
                                                                   taskExecutorService);
      try {
        scheduleStore.updateLastRun(programId, programType, streamSizeSchedule.getName(),
                                    pollingInfo.getSize(), pollingInfo.getTimestamp(),
                                    new DatasetBasedStreamSizeScheduleStore.TransactionMethod() {
                                      @Override
                                      public void execute() throws Exception {
                                        LOG.info("About to start streamSizeSchedule {}", currentSchedule.getName());
                                        taskRunner.run(programId, ProgramType.valueOf(programType.name()),
                                                       argsBuilder.build());
                                      }
                                    });
        lastRunSize = pollingInfo.getSize();
        lastRunTs = pollingInfo.getTimestamp();
      } catch (Throwable t) {
        LOG.error("Error when persisting last run information for schedule {} in store",
                  streamSizeSchedule.getName(), t);
      }
    }

    /**
     * @return true if we successfully suspended the schedule, false if it was already suspended
     */
    public synchronized boolean suspend() throws SchedulerException {
      if (active.compareAndSet(true, false)) {
        try {
          scheduleStore.suspend(programId, programType, streamSizeSchedule.getName());
        } catch (Throwable t) {
          // Roll back change
          active.set(true);
          throw new SchedulerException("Error when suspending schedule " + streamSizeSchedule.getName() + "in store",
                                       t);
        }
        return true;
      }
      return false;
    }

    /**
     * @return true if we successfully resumed the schedule, false if it was already active
     */
    public synchronized boolean resume() throws SchedulerException {
      if (active.compareAndSet(false, true)) {
        try {
          scheduleStore.resume(programId, programType, streamSizeSchedule.getName());
          return true;
        } catch (Throwable t) {
          // Roll back change
          active.set(false);
          throw new SchedulerException("Error when resuming schedule " + streamSizeSchedule.getName() + "in store",
                                       t);
        }
      }
      return false;
    }

    /**
     * Replace the {@link StreamSizeSchedule} of this task.
     */
    public synchronized void updateSchedule(StreamSizeSchedule schedule) throws SchedulerException {
      if (!schedule.equals(streamSizeSchedule)) {
        try {
          scheduleStore.updateSchedule(programId, programType, streamSizeSchedule.getName(), schedule);
        } catch (Throwable t) {
          throw new SchedulerException("Error when updating schedule " + streamSizeSchedule.getName() + "in store",
                                       t);
        }
        streamSizeSchedule = schedule;
      }
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

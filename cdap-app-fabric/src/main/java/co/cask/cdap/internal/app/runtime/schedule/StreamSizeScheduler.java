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
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.schedule.StreamSizeSchedule;
import co.cask.cdap.notifications.feeds.NotificationFeedException;
import co.cask.cdap.notifications.feeds.NotificationFeedNotFoundException;
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
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;

/**
 * {@link Scheduler} that triggers program executions based on data availability in streams.
 */
@Singleton
public class StreamSizeScheduler implements Scheduler {
  private static final Logger LOG = LoggerFactory.getLogger(StreamSizeScheduler.class);
  private static final int STREAM_POLLING_THREAD_POOL_SIZE = 10;

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
  }

  public void stop() {
    streamPollingExecutor.shutdownNow();
    for (StreamSubscriber subscriber : streamSubscribers.values()) {
      subscriber.cancel();
    }
  }

  @Override
  public void schedule(Id.Program program, SchedulableProgramType programType, Schedule schedule)
    throws SchedulerException {
    Preconditions.checkArgument(schedule instanceof StreamSizeSchedule,
                                "Schedule should be of type StreamSizeSchedule");
    StreamSizeSchedule streamSizeSchedule = (StreamSizeSchedule) schedule;
    schedule(program, programType, streamSizeSchedule, true, -1, -1, true);
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
   * @param persist {@code true} if this schedule should be persisted in the persistent store containing the
   *                stream size schedules, {@code false} otherwise
   */
  private void schedule(Id.Program program, SchedulableProgramType programType, StreamSizeSchedule streamSizeSchedule,
                        boolean active, long basePollSize, long basePollTs, boolean persist)
    throws SchedulerException  {
    // Create a new StreamSubscriber, if one doesn't exist for the stream passed in the schedule
    Id.Stream streamId = Id.Stream.from(program.getNamespaceId(), streamSizeSchedule.getStreamName());
    StreamSubscriber streamSubscriber = streamSubscribers.get(streamId);
    if (streamSubscriber == null) {
      streamSubscriber = new StreamSubscriber(streamId);
      StreamSubscriber previous = streamSubscribers.putIfAbsent(streamId, streamSubscriber);
      if (previous == null) {
        try {
          streamSubscriber.start();
        } catch (NotificationFeedException e) {
          streamSubscribers.remove(streamId);
          LOG.error("Notification feed error for streamSizeSchedule {}", streamSizeSchedule);
          throw new SchedulerException(e);
        } catch (NotificationFeedNotFoundException e) {
          streamSubscribers.remove(streamId);
          LOG.error("Notification feed does not exist for streamSizeSchedule {}", streamSizeSchedule);
          throw new SchedulerException(e);
        }
      } else {
        streamSubscriber = previous;
      }
    }

    // Add the scheduleTask to the StreamSubscriber
    if (streamSubscriber.createScheduleTask(program, programType, streamSizeSchedule,
                                            active, basePollSize, basePollTs, persist)) {
      scheduleSubscribers.put(AbstractSchedulerService.scheduleIdFor(program, programType,
                                                                     streamSizeSchedule.getName()),
                              streamSubscriber);
    }
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
                                                           String.format("%s%c", programScheduleId, endChar))
                                  .keySet());
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
      try {
        deleteSchedule(program, programType, schedule.getName());
      } catch (NotFoundException e) {
        // It can happen that the schedule was deleted while being updated. In which case, the update action
        // came first and we still want to create it
        LOG.warn("Schedule {} deleted while being updated", schedule.getName(), e);
      }
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

  private synchronized Store getStore() {
    if (store == null) {
      store = storeFactory.create();
    }
    return store;
  }

  /**
   * One instance of this class contains a list of {@link StreamSizeSchedule}s, which are all interested
   * in the same stream. This instance subscribes to the size notification of the stream, and polls the
   * stream for its size whenever the schedules it references need the information.
   * The {@link StreamSizeScheduler} communicates with this class, which in turn communicates to the schedules
   * it contains to perform operations on the schedules - suspend, resume, etc.
   */
  private final class StreamSubscriber implements NotificationHandler<StreamSizeNotification>, Cancellable {
    // Key is the schedule ID
    private final ConcurrentMap<String, StreamSizeScheduleTask> scheduleTasks;
    private final Object lastPollingInfoLock;
    private final Id.Stream streamId;
    private final AtomicInteger activeTasks;

    private Cancellable notificationSubscription;
    private ScheduledFuture<?> scheduledPolling;
    private StreamSizeNotification lastNotification;
    private StreamSizeNotification lastPollingInfo;

    private StreamSubscriber(Id.Stream streamId) {
      this.streamId = streamId;
      this.scheduleTasks = Maps.newConcurrentMap();
      this.lastPollingInfoLock = new Object();
      this.activeTasks = new AtomicInteger(0);
    }

    public void start() throws NotificationFeedException, NotificationFeedNotFoundException {
      notificationSubscription = notificationService.subscribe(getFeed(), this, sendPollingInfoExecutor);
    }

    @Override
    public void cancel() {
      if (scheduledPolling != null) {
        scheduledPolling.cancel(true);
      }
      if (notificationSubscription != null) {
        notificationSubscription.cancel();
      }
    }

    /**
     * Add a new scheduling task based on the data received by the stream referenced by {@code this} object.
     * @return {@code true} if the task was created successfully, {@code false} if it already exists
     */
    public boolean createScheduleTask(Id.Program programId, SchedulableProgramType programType,
                                      StreamSizeSchedule streamSizeSchedule, boolean active,
                                      long baseRunSize, long baseRunTs, boolean persist) {
      // TODO add a createScheduleTasks, so that if we create multiple schedules for the same stream at the same
      // time, we don't have to poll the stream many times

      StreamSizeScheduleTask newTask = new StreamSizeScheduleTask(programId, programType, streamSizeSchedule);
      synchronized (this) {
        StreamSizeScheduleTask previous =
          scheduleTasks.putIfAbsent(AbstractSchedulerService.scheduleIdFor(programId, programType,
                                                                           streamSizeSchedule.getName()),
                                    newTask);
        if (previous != null) {
          // We cannot replace an existing schedule - that functionality is not wanted - yet
          return false;
        }

        if (active) {
          activeTasks.incrementAndGet();
        }
      }

      // Initialize the schedule task
      if (baseRunSize == -1 && baseRunTs == -1) {
        // This is the first time that we schedule this task - ie it was not in the schedule store
        // before. Hence we set the base metrics properly
        try {
          StreamSize streamSize = queryStreamEventsSize();
          cancelPollingAndScheduleNext();
          newTask.startSchedule(streamSize.getSize(), streamSize.getTimestamp(), active, persist);
          synchronized (lastPollingInfoLock) {
            if (lastPollingInfo == null || lastPollingInfo.getTimestamp() < streamSize.getTimestamp()) {
              lastPollingInfo = new StreamSizeNotification(streamSize.getTimestamp(), streamSize.getSize());
            }
          }
          sendPollingInfoToActiveTasks(lastPollingInfo);
        } catch (IOException e) {
          // In case polling the stream events size failed, we can initialize the schedule task to the last notification
          // info if it exists, or to 0. This won't be very accurate, but this is the best info we have at that time
          synchronized (lastPollingInfoLock) {
            if (lastPollingInfo != null) {
              newTask.startSchedule(lastPollingInfo.getSize(), lastPollingInfo.getTimestamp(), active, persist);
              newTask.receivedPollingInformation(lastPollingInfo);
            } else {
              newTask.startSchedule(0L, System.currentTimeMillis(), active, persist);
              if (lastNotification != null) {
                newTask.receivedNotification(lastNotification);
              }
            }
          }
        }
      } else {
        newTask.startSchedule(baseRunSize, baseRunTs, active, persist);
        if (lastPollingInfo != null && lastNotification != null) {
          if (lastPollingInfo.getTimestamp() >= lastNotification.getTimestamp()) {
            sendPollingInfoToActiveTasks(lastPollingInfo);
          } else {
            sendNotificationToActiveTasks(lastNotification);
          }
        } else if (lastNotification == null && lastPollingInfo != null) {
          sendPollingInfoToActiveTasks(lastPollingInfo);
        } else if (lastNotification != null) {
          sendNotificationToActiveTasks(lastNotification);
        }
      }
      return true;
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
    public void resumeScheduleTask(Id.Program programId, SchedulableProgramType programType,
                                                String scheduleName) throws ScheduleNotFoundException {
      final StreamSizeScheduleTask task;
      synchronized (this) {
        String scheduleId = AbstractSchedulerService.scheduleIdFor(programId, programType, scheduleName);
        task = scheduleTasks.get(scheduleId);
        if (task == null) {
          throw new ScheduleNotFoundException(scheduleName);
        }
        if (!task.resume()) {
          return;
        }
        if (activeTasks.incrementAndGet() == 1) {
          // There were no active tasks until then, that means polling the stream was disabled.
          // We need to check if it is necessary to poll the stream at this time, if the last
          // notification received was too long ago, or if there is no last seen notification
          synchronized (lastPollingInfoLock) {
            if (lastPollingInfo == null ||
              (lastPollingInfo.getTimestamp() + pollingDelay <= System.currentTimeMillis())) {
              // Resume stream polling
              cancelPollingAndScheduleNext();
              try {
                StreamSize streamSize = queryStreamEventsSize();
                lastPollingInfo = new StreamSizeNotification(streamSize.getTimestamp(), streamSize.getSize());
              } catch (IOException e) {
                LOG.debug("Ignoring stream events size polling after resuming schedule {} due to error",
                          scheduleName, e);
              }
            }
          }
        }
      }

      // We need to send the last up to date information to the schedule task
      // We know at least here that lastPollingInfo is not null, since we updated it in the synchronized block
      if (lastNotification == null || lastPollingInfo.getTimestamp() >= lastNotification.getTimestamp()) {
        sendPollingInfoExecutor.execute(new Runnable() {
          @Override
          public void run() {
            task.receivedPollingInformation(lastPollingInfo);
          }
        });
      } else {
        task.receivedNotification(lastNotification);
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

      // We need to send the last up to date information to the schedule task
      if (lastNotification == null && lastPollingInfo != null ||
        lastNotification != null && lastPollingInfo != null &&
          lastPollingInfo.getTimestamp() >= lastNotification.getTimestamp()) {
        sendPollingInfoExecutor.execute(new Runnable() {
          @Override
          public void run() {
            scheduleTask.receivedPollingInformation(lastPollingInfo);
          }
        });
      } else if (lastNotification != null) {
        scheduleTask.receivedNotification(lastNotification);
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

    @Override
    public Type getNotificationFeedType() {
      return StreamSizeNotification.class;
    }

    @Override
    public void received(StreamSizeNotification notification, NotificationContext notificationContext) {
      // We only pass the stream size notification to the schedule tasks if the notification
      // came after the last seen notification
      boolean send = false;
      if (activeTasks.get() > 0) {
        cancelPollingAndScheduleNext();
      }
      if (lastNotification == null || notification.getTimestamp() > lastNotification.getTimestamp()) {
        send = true;
        lastNotification = notification;
      }
      if (send) {
        sendNotificationToActiveTasks(notification);
      }
    }

    /**
     * Send a {@link StreamSizeNotification} built using information from the stream metrics to all the active
     * {@link StreamSizeSchedule} referenced by this object.
     */
    private void sendPollingInfoToActiveTasks(final StreamSizeNotification pollingInfo) {
      lastPollingInfo = pollingInfo;
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

    /**
     * Send a {@link StreamSizeNotification} coming from the notification service to all the active
     * {@link StreamSizeSchedule} referenced by this object.
     */
    private void sendNotificationToActiveTasks(StreamSizeNotification notification) {
      boolean triggerPolling = false;
      for (final StreamSizeScheduleTask task : scheduleTasks.values()) {
        if (!task.isActive()) {
          continue;
        }
        triggerPolling = triggerPolling | task.receivedNotification(notification);
      }
      if (triggerPolling) {
        try {
          StreamSize streamSize = queryStreamEventsSize();
          sendPollingInfoToActiveTasks(new StreamSizeNotification(streamSize.getTimestamp(), streamSize.getSize()));
        } catch (IOException e) {
          LOG.warn("Could not poll stream {} size using metrics", streamId, e);
        }
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
     * Cancel the currently scheduled stream size polling task, and reschedule one for later.
     */
    private synchronized void cancelPollingAndScheduleNext() {
      if (scheduledPolling != null) {
        // This method might be called from the run method defined in the below Runnable - in which case
        // this scheduledPolling would in fact be active. Hence we don't want to interrupt the active schedulePolling
        // future by passing true to the cancel method
        scheduledPolling.cancel(false);
      }

      // Regardless of whether cancelling was successful, we still want to schedule the next polling
      scheduledPolling = streamPollingExecutor.schedule(createPollingRunnable(), pollingDelay, TimeUnit.MILLISECONDS);
    }

    /**
     * @return a runnable that uses the {@link StreamAdmin} to poll the stream size, and creates a fake notification
     *         with that size, so that this information can be treated as if it came from a real notification.
     */
    private Runnable createPollingRunnable() {
      return new Runnable() {
        @Override
        public void run() {
          // We only perform polling if at least one scheduleTask is active
          if (activeTasks.get() > 0) {
            try {
              StreamSize streamSize = queryStreamEventsSize();
              cancelPollingAndScheduleNext();
              sendPollingInfoToActiveTasks(new StreamSizeNotification(streamSize.getTimestamp(), streamSize.getSize()));
            } catch (IOException e) {
              LOG.debug("Ignoring stream events size polling after error", e);
            }
          }
        }
      };
    }

    /**
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
    private StreamSizeSchedule streamSizeSchedule;
    private AtomicBoolean active;

    // Size, in bytes, given by the notification which serves as a base when comparing notifications
    private long baseNotificationSize;
    // Size, in bytes, given by the last received notification
    private long lastNotificationSize;

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
    }

    /**
     * Start a stream size schedule task.
     *
     * @param basePollSize base size of the stream to start counting from. This info comes from polling the stream
     * @param basePollTs time at which the {@code basePollSize} was obtained
     * @param active {@code true} if the schedule is active, {@code false} if it is suspended
     * @param persist {@code true} if this information should be persisted, {@code false} otherwise
     */
    public void startSchedule(long basePollSize, long basePollTs, boolean active, boolean persist) {
      LOG.debug("Starting schedule {} with basePollSize {}, basePollTs {}, active {}. Should be persisted: {}",
                streamSizeSchedule.getName(), basePollSize, basePollTs, active, persist);
      this.basePollSize = basePollSize;
      this.basePollTs = basePollTs;
      this.baseNotificationSize = -1;
      this.lastNotificationSize = -1;
      this.lastRunSize = -1;
      this.lastRunTs = -1;
      this.active = new AtomicBoolean(active);
    }

    public boolean isActive() {
      return active.get();
    }

    /**
     * Received stream size information coming from the notification service. We are guaranteed to receive
     * notifications in chronological order.
     *
     * @param notification {@link StreamSizeNotification} info that came from the notification service
     * @return {@code true} if the stream should be polled to get more accurate info on it. {@code false} if we
     *         don't need to
     */
    public boolean receivedNotification(@Nonnull StreamSizeNotification notification) {
      Preconditions.checkNotNull(notification);
      if (!active.get()) {
        return false;
      }

      synchronized (this) {
        long tmpLastNotificationSize = lastNotificationSize;
        lastNotificationSize = notification.getSize();
        if (tmpLastNotificationSize == -1) {
          // Trigger polling of stream, so that we can recalibrate the base notification size in the
          // receivedPollingInformation method
          return true;
        } else if (baseNotificationSize != -1 &&
          notification.getSize() >= baseNotificationSize + toBytes(streamSizeSchedule.getDataTriggerMB())) {
          return true;
        }
      }
      // In the case where lastNotificationSize != -1 but baseNotificationSize == -1,
      // there is nothing to do, because the previous condition lastNotificationSize == -1
      // has already been met at an earlier time, and we are waiting for a polling now to
      // recalibrate the base notification size
      return false;
    }

    /**
     * Received stream size information coming from polling.
     *
     * @param pollingInfo {@link StreamSizeNotification} info that came from polling the stream using metrics
     */
    public void receivedPollingInformation(@Nonnull StreamSizeNotification pollingInfo) {
      Preconditions.checkNotNull(pollingInfo);
      if (!active.get()) {
        return;
      }

      StreamSizeSchedule currentSchedule;
      ImmutableMap.Builder<String, String> argsBuilder = ImmutableMap.builder();
      synchronized (this) {
        if (pollingInfo.getSize() < basePollSize) {
          // Polling the stream showed less data, that means something bad happened to metrics:
          // either it has TTLed, or has been deleted. In any case, we can't do much but to reset
          // the base poll size to the last know info
          basePollSize = pollingInfo.getSize();
          LOG.warn("Size metric has decreased for stream {}. Scheduling logic for schedule '{}' will " +
                     "restart from size {}",
                   streamSizeSchedule.getStreamName(), streamSizeSchedule.getName(), basePollSize);
          return;
        }

        long dataTriggerBytes = toBytes(streamSizeSchedule.getDataTriggerMB());
        long delta = pollingInfo.getSize() - basePollSize;
        if (lastNotificationSize != -1) {
          // We recalibrate the base notification to be:
          // last notification minus the delta observed between the current polling info and the base polling info
          baseNotificationSize = Math.max(0L, lastNotificationSize - (delta % dataTriggerBytes));
        }

        if (delta < dataTriggerBytes) {
          return;
        }

        currentSchedule = streamSizeSchedule;
        basePollSize = pollingInfo.getSize();
        lastRunSize = pollingInfo.getSize();
        basePollTs = pollingInfo.getTimestamp();
        lastRunTs = pollingInfo.getTimestamp();

        argsBuilder.put(ProgramOptionConstants.SCHEDULE_NAME, streamSizeSchedule.getName());
        argsBuilder.put(ProgramOptionConstants.LOGICAL_START_TIME, Long.toString(pollingInfo.getTimestamp()));
        argsBuilder.put(ProgramOptionConstants.RUN_DATA_SIZE, Long.toString(pollingInfo.getSize()));
        argsBuilder.put(ProgramOptionConstants.RUN_BASE_COUNT_TIME, Long.toString(basePollTs));
        argsBuilder.put(ProgramOptionConstants.RUN_BASE_COUNT_SIZE, Long.toString(basePollSize));

        if (lastRunSize != -1 && lastRunTs != -1) {
          argsBuilder.put(ProgramOptionConstants.PAST_RUN_LOGICAL_START_TIME, Long.toString(lastRunTs));
          argsBuilder.put(ProgramOptionConstants.PAST_RUN_DATA_SIZE, Long.toString(lastRunSize));
        }
      }

      ScheduleTaskRunner taskRunner = new ScheduleTaskRunner(getStore(), programRuntimeService, preferencesStore);
      try {
        LOG.info("About to start streamSizeSchedule {}", currentSchedule.getName());
        taskRunner.run(programId, ProgramType.valueOf(programType.name()), new BasicArguments(argsBuilder.build()));
      } catch (TaskExecutionException e) {
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

    private long toBytes(int mb) {
      return ((long) mb) * 1024 * 1024;
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

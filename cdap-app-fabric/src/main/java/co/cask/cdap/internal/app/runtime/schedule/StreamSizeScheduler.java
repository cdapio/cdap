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

import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.schedule.Schedule;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.app.store.StoreFactory;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * {@link Scheduler} that triggers program executions based on data availability in streams.
 */
@Singleton
public class StreamSizeScheduler implements Scheduler {
  private static final Logger LOG = LoggerFactory.getLogger(StreamSizeScheduler.class);
  private static final int STREAM_POLLING_THREAD_POOL_SIZE = 10;

  private final long pollingDelay;
  private final NotificationService notificationService;
  private final StreamAdmin streamAdmin;
  private final StoreFactory storeFactory;
  private final ProgramRuntimeService programRuntimeService;
  private final PreferencesStore preferencesStore;
  private final ConcurrentMap<Id.Stream, StreamSubscriber> streamSubscribers;

  // Key is scheduleId
  private final ConcurrentMap<String, StreamSubscriber> scheduleSubscribers;

  private Store store;
  private Executor notificationExecutor;
  private ScheduledExecutorService streamPollingExecutor;

  @Inject
  public StreamSizeScheduler(CConfiguration cConf, NotificationService notificationService, StreamAdmin streamAdmin,
                             StoreFactory storeFactory, ProgramRuntimeService programRuntimeService,
                             PreferencesStore preferencesStore) {
    this.pollingDelay = cConf.getLong(Constants.Notification.Stream.STREAM_SIZE_SCHEDULE_POLLING_DELAY);
    this.notificationService = notificationService;
    this.streamAdmin = streamAdmin;
    this.storeFactory = storeFactory;
    this.programRuntimeService = programRuntimeService;
    this.preferencesStore = preferencesStore;
    this.streamSubscribers = Maps.newConcurrentMap();
    this.scheduleSubscribers = Maps.newConcurrentMap();
    this.store = null;
  }

  public void start() {
    notificationExecutor = Executors.newCachedThreadPool(Threads.createDaemonThreadFactory("stream-size-scheduler-%d"));
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
  public void schedule(Id.Program program, SchedulableProgramType programType, Schedule schedule) {
    Preconditions.checkArgument(schedule instanceof StreamSizeSchedule,
                                "Schedule should be of type StreamSizeSchedule");
    StreamSizeSchedule streamSizeSchedule = (StreamSizeSchedule) schedule;
    schedule(program, programType, streamSizeSchedule, true, -1, -1, true);
  }

  private void schedule(Id.Program program, SchedulableProgramType programType, StreamSizeSchedule streamSizeSchedule,
                        boolean active, long baseRunSize, long baseRunTs, boolean persist) {
    try {
      // Create a new StreamSubscriber, if one doesn't exist for the stream passed in the schedule
      Id.Stream streamId = Id.Stream.from(program.getNamespaceId(), streamSizeSchedule.getStreamName());
      StreamSubscriber streamSubscriber = new StreamSubscriber(streamId);
      StreamSubscriber previous = streamSubscribers.putIfAbsent(streamId, streamSubscriber);
      if (previous == null) {
        streamSubscriber.start();
      } else {
        streamSubscriber = previous;
      }

      // Add the scheduleTask to the StreamSubscriber
      streamSubscriber.createScheduleTask(program, programType, streamSizeSchedule, active, baseRunSize, baseRunTs,
                                          persist);
      scheduleSubscribers.put(getScheduleId(program, programType, streamSizeSchedule.getName()),
                              streamSubscriber);
    } catch (NotificationFeedException e) {
      LOG.error("Notification feed does not exist for streamSizeSchedule {}", streamSizeSchedule);
      throw Throwables.propagate(e);
    } catch (NotificationFeedNotFoundException e) {
      LOG.error("Notification feed does not exist for streamSizeSchedule {}", streamSizeSchedule);
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void schedule(Id.Program program, SchedulableProgramType programType, Iterable<Schedule> schedules) {
    for (Schedule s : schedules) {
      schedule(program, programType, s);
    }
  }

  @Override
  public List<ScheduledRuntime> nextScheduledRuntime(Id.Program program, SchedulableProgramType programType) {
    return ImmutableList.of();
  }

  @Override
  public List<String> getScheduleIds(Id.Program program, SchedulableProgramType programType) {
    Set<String> scheduleIds;
    synchronized (this) {
      scheduleIds = ImmutableSet.copyOf(scheduleSubscribers.keySet());
    }
    ImmutableList.Builder<String> builder = ImmutableList.builder();
    String programScheduleId = getProgramScheduleId(program, programType);
    for (String scheduleId : scheduleIds) {
      if (scheduleId.startsWith(programScheduleId)) {
        builder.add(scheduleId);
      }
    }
    return builder.build();
  }

  @Override
  public void suspendSchedule(Id.Program program, SchedulableProgramType programType, String scheduleName) {
    StreamSubscriber subscriber = scheduleSubscribers.get(getScheduleId(program, programType, scheduleName));
    if (subscriber != null) {
      subscriber.suspendScheduleTask(program, programType, scheduleName);
    }
  }

  @Override
  public void resumeSchedule(Id.Program program, SchedulableProgramType programType, String scheduleName) {
    StreamSubscriber subscriber = scheduleSubscribers.get(getScheduleId(program, programType, scheduleName));
    if (subscriber != null) {
      subscriber.resumeScheduleTask(program, programType, scheduleName);
    }
  }

  @Override
  public void deleteSchedule(Id.Program programId, SchedulableProgramType programType, String scheduleName) {
    StreamSubscriber subscriber = scheduleSubscribers.remove(getScheduleId(programId, programType, scheduleName));
    if (subscriber != null) {
      synchronized (this) {
        subscriber.deleteSchedule(programId, programType, scheduleName);
        if (subscriber.isEmpty()) {
          subscriber.cancel();
          Id.Stream streamId = subscriber.getStreamId();
          streamSubscribers.remove(streamId);
        }
      }
    }
  }

  @Override
  public void deleteSchedules(Id.Program programId, SchedulableProgramType programType) {
    Set<String> scheduleIds;
    synchronized (this) {
      scheduleIds = ImmutableSet.copyOf(scheduleSubscribers.keySet());
    }
    String programScheduleId = getProgramScheduleId(programId, programType);
    for (String scheduleId : scheduleIds) {
      if (scheduleId.startsWith(programScheduleId)) {
        int idx = scheduleId.lastIndexOf(':');
        deleteSchedule(programId, programType, scheduleId.substring(idx + 1));
      }
    }
  }

  @Override
  public ScheduleState scheduleState(Id.Program program, SchedulableProgramType programType, String scheduleName) {
    StreamSubscriber subscriber = scheduleSubscribers.get(getScheduleId(program, programType, scheduleName));
    if (subscriber != null) {
      return subscriber.scheduleTaskState(program, programType, scheduleName);
    } else {
      return ScheduleState.NOT_FOUND;
    }
  }

  private Store getStore() {
    if (store == null) {
      store = storeFactory.create();
    }
    return store;
  }

  private String getScheduleId(Id.Program program, SchedulableProgramType programType, String scheduleName) {
    return String.format("%s:%s", getProgramScheduleId(program, programType), scheduleName);
  }

  private String getProgramScheduleId(Id.Program program, SchedulableProgramType programType) {
    return String.format("%s:%s:%s:%s", program.getNamespaceId(), program.getApplicationId(),
                         programType.name(), program.getId());
  }

  /**
   * One instance of this class contains a list of {@link StreamSizeSchedule}s, which are all interested
   * in the same stream. This instance subscribes to the size notification of the stream, and polls the
   * stream for its size whenever the schedules it references need the information.
   * The {@link StreamSizeScheduler} communicates with this class, which in turn communicates to the schedules
   * it contains to perform operations on the schedules - suspend, resume, etc.
   */
  private final class StreamSubscriber implements NotificationHandler<StreamSizeNotification>, Cancellable {

    private final Id.Stream streamId;

    // Key is the schedule ID
    private final ConcurrentMap<String, StreamSizeScheduleTask> scheduleTasks;

    private Cancellable notificationSubscription;
    private ScheduledFuture<?> scheduledPolling;
    private StreamSizeNotification lastNotification;
    private int activeTasks;

    private StreamSubscriber(Id.Stream streamId) {
      this.streamId = streamId;
      this.scheduleTasks = Maps.newConcurrentMap();
      this.activeTasks = 0;
    }

    public void start() throws NotificationFeedException, NotificationFeedNotFoundException {
      notificationSubscription = notificationService.subscribe(getFeed(), this, notificationExecutor);
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
     */
    public void createScheduleTask(Id.Program programId, SchedulableProgramType programType,
                                   StreamSizeSchedule streamSizeSchedule, boolean active,
                                   long baseRunSize, long baseRunTs, boolean persist) {
      // TODO add a createScheduleTasks, so that if we create multiple schedules for the same stream at the same
      // time, we don't have to poll the stream many times

      StreamSizeScheduleTask newTask = new StreamSizeScheduleTask(programId, programType, streamSizeSchedule);
      synchronized (this) {
        StreamSizeScheduleTask previous =
          scheduleTasks.putIfAbsent(getScheduleId(programId, programType, streamSizeSchedule.getName()),
                                    newTask);
        if (previous == null) {
          if (active) {
            activeTasks++;
          }

          // Initialize the schedule task
          if (baseRunSize == -1 && baseRunTs == -1) {
            // This is likely to be the first time that we schedule this task - ie it was not in the schedule store
            // before. Hence we set the base metrics properly
            long baseTs = System.currentTimeMillis();
            long baseSize = pollStream();
            newTask.startSchedule(baseSize, baseTs, active, persist);
            lastNotification = new StreamSizeNotification(baseTs, baseSize);
            received(lastNotification, null);
          } else {
            newTask.startSchedule(baseRunSize, baseRunTs, active, persist);
          }
        }
      }
    }

    /**
     * Suspend a scheduling task that is based on the data received by the stream referenced by {@code this} object.
     */
    public void suspendScheduleTask(Id.Program programId, SchedulableProgramType programType, String scheduleName) {
      StreamSizeScheduleTask task = scheduleTasks.get(getScheduleId(programId, programType, scheduleName));
      if (task == null) {
        return;
      }
      synchronized (this) {
        if (task.suspend()) {
          activeTasks--;
        }
      }
    }

    /**
     * Resume a scheduling task that is based on the data received by the stream referenced by {@code this} object.
     */
    public void resumeScheduleTask(Id.Program programId, SchedulableProgramType programType, String scheduleName) {
      synchronized (this) {
        StreamSizeScheduleTask task = scheduleTasks.get(getScheduleId(programId, programType, scheduleName));
        if (task == null) {
          return;
        }
        if (task.resume() && ++activeTasks == 1) {
          // There were no active tasks until then, that means polling the stream was disabled.
          // We need to check if it is necessary to poll the stream at this time, if the last
          // notification received was too long ago

          // lastNotification cannot be null, since when creating one scheduleTask, we instantiate it
          // TODO this will change once we have a schedule store, and one schedule can be added that is suspended
          // TODO test that configuration
          if (lastNotification != null) {
            long lastNotificationTs = lastNotification.getTimestamp();
            if (lastNotificationTs + TimeUnit.SECONDS.toMillis(pollingDelay) <= System.currentTimeMillis()) {
              long streamSize = pollStream();
              lastNotification = new StreamSizeNotification(System.currentTimeMillis(), streamSize);
            }
          }
        }

        if (lastNotification != null) {
          task.received(lastNotification);
        }
      }
    }

    /**
     * Delete a scheduling task that is based on the data received by the stream referenced by {@code this} object.
     */
    public void deleteSchedule(Id.Program programId, SchedulableProgramType programType, String scheduleName) {
      StreamSizeScheduleTask scheduleTask = scheduleTasks.remove(getScheduleId(programId, programType, scheduleName));
      if (scheduleTask != null && scheduleTask.isRunning()) {
        activeTasks--;
      }
    }

    /**
     * Get the status a scheduling task that is based on the data received by the stream referenced by {@code this}
     * object.
     */
    public ScheduleState scheduleTaskState(Id.Program programId, SchedulableProgramType programType,
                                           String scheduleName) {
      StreamSizeScheduleTask task = scheduleTasks.get(getScheduleId(programId, programType, scheduleName));
      if (task == null) {
        return ScheduleState.NOT_FOUND;
      }
      return task.isRunning() ? ScheduleState.SCHEDULED : ScheduleState.SUSPENDED;
    }

    /**
     * @return true if this object does not reference any schedule, false otherwise
     */
    public boolean isEmpty() {
      return scheduleTasks.isEmpty();
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
      cancelPollingAndScheduleNext();
      if (lastNotification == null || notification.getTimestamp() > lastNotification.getTimestamp()) {
        lastNotification = notification;
        sendNotificationToActiveTasks(notification);
      }
    }

    /**
     * Send a {@link StreamSizeNotification} to all the active {@link StreamSizeSchedule} referenced
     * by this object.
     */
    private void sendNotificationToActiveTasks(final StreamSizeNotification notification) {
      List<StreamSizeScheduleTask> values;
      synchronized (this) {
        values = ImmutableList.copyOf(scheduleTasks.values());
      }

      for (final StreamSizeScheduleTask task : values) {
        if (!task.isRunning()) {
          continue;
        }
        notificationExecutor.execute(new Runnable() {
          @Override
          public void run() {
            task.received(notification);
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
     * Cancel the currently scheduled stream size polling task, and reschedule one for later.
     */
    private void cancelPollingAndScheduleNext() {
      // This method might be called from the call to #received defined in the below Runnable - in which case
      // this scheduledPolling would in fact be running. Hence we don't want to interrupt the running task
      if (scheduledPolling != null) {
        scheduledPolling.cancel(false);
      }

      // Regardless of whether cancelling was successful, we still want to schedule the next polling
      scheduledPolling = streamPollingExecutor.schedule(createPollingRunnable(), pollingDelay, TimeUnit.SECONDS);
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
          if (activeTasks > 0) {
            long size = pollStream();

            // We don't need a notification context here
            received(new StreamSizeNotification(System.currentTimeMillis(), size), null);
          }
        }
      };
    }

    /**
     * @return size of the stream queried directly from the file system
     */
    private long pollStream() {
      try {
        // Note we can't store the stream config, because its generation might change at every moment
        return streamAdmin.fetchStreamSize(streamAdmin.getConfig(streamId));
      } catch (IOException e) {
        LOG.error("Could not poll size for stream {}", streamId);
        throw Throwables.propagate(e);
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
    private final StreamSizeSchedule streamSizeSchedule;

    private long baseSize;
    private long baseTs;
    private boolean running;

    private StreamSizeScheduleTask(Id.Program programId, SchedulableProgramType programType,
                                   StreamSizeSchedule streamSizeSchedule) {
      this.programId = programId;
      this.programType = programType;
      this.streamSizeSchedule = streamSizeSchedule;
    }

    public void startSchedule(long baseSize, long baseTs, boolean active, boolean persist) {
      LOG.debug("Starting schedule {} with baseSize {}, baseTs {}, active {}. Should be persisted: {}",
                streamSizeSchedule.getName(), baseSize, baseTs, active, persist);
      this.baseSize = baseSize;
      this.baseTs = baseTs;
      this.running = active;
    }

    public boolean isRunning() {
      return running;
    }

    public void received(StreamSizeNotification notification) {
      long pastRunSize;
      long pastRunTs;
      synchronized (this) {
        if (notification.getSize() < baseSize) {
          // This can happen when a stream is truncated: the baseSize is still the old size,
          // but we receive notification with way less data
          baseSize = notification.getSize();
          baseTs = notification.getTimestamp();
          return;
        }
        if (notification.getSize() < baseSize + toBytes(streamSizeSchedule.getDataTriggerMB())) {
          return;
        }

        // Update the baseSize as soon as possible to avoid races
        pastRunSize = baseSize;
        pastRunTs = baseTs;
        baseSize = notification.getSize();
        baseTs = notification.getTimestamp();
        LOG.debug("Base size and ts updated to {}, {} for streamSizeSchedule {}",
                  baseSize, baseTs, streamSizeSchedule);
      }

      Arguments args = new BasicArguments(ImmutableMap.of(
        ProgramOptionConstants.SCHEDULE_NAME, streamSizeSchedule.getName(),
        ProgramOptionConstants.LOGICAL_START_TIME, Long.toString(baseTs),
        ProgramOptionConstants.RUN_DATA_SIZE, Long.toString(baseSize),
        ProgramOptionConstants.PAST_RUN_LOGICAL_START_TIME, Long.toString(pastRunTs),
        ProgramOptionConstants.PAST_RUN_DATA_SIZE, Long.toString(pastRunSize)
      ));

      while (true) {
        ScheduleTaskRunner taskRunner = new ScheduleTaskRunner(getStore(), programRuntimeService, preferencesStore);
        try {
          LOG.info("About to start streamSizeSchedule {}", streamSizeSchedule);
          taskRunner.run(programId, ProgramType.valueOf(programType.name()), args);
          break;
        } catch (TaskExecutionException e) {
          LOG.error("Execution exception while running streamSizeSchedule {}", streamSizeSchedule.getName(), e);
          if (e.isRefireImmediately()) {
            LOG.info("Retrying execution for streamSizeSchedule {}", streamSizeSchedule.getName());
          } else {
            break;
          }
        }
      }
    }

    /**
     * @return true if we successfully suspended the schedule, false if it was already suspended
     */
    public boolean suspend() {
      if (!running) {
        return false;
      }
      running = false;
      return true;
    }

    /**
     * @return true if we successfully resumed the schedule, false if it was already running
     */
    public boolean resume() {
      if (running) {
        return false;
      }
      running = true;
      return true;
    }

    private long toBytes(int mb) {
      return ((long) mb) * 1024 * 1024;
    }
  }
}

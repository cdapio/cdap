package co.cask.cdap.internal.app.services;

import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.app.store.RuntimeStore;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.proto.BasicThrowable;
import co.cask.cdap.proto.Notification;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.id.ProgramId;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import org.apache.tephra.TransactionSystemClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.Map;

/**
 * Service that receives program statuses and persists to the store
 */
public class ProgramStatusService extends AbstractNotificationSubscriberService {
  private static final Logger LOG = LoggerFactory.getLogger(ProgramStatusService.class);
  private static final Gson GSON = new Gson();
  private static final Type STRING_STRING_MAP = new TypeToken<Map<String, String>>() { }.getType();

  @Inject
  ProgramStatusService(MessagingService messagingService, RuntimeStore runtimeStore, CConfiguration cConf,
                                        DatasetFramework datasetFramework, TransactionSystemClient txClient) {
    super(messagingService, runtimeStore, cConf, datasetFramework, txClient);
  }

  @Override
  protected void startUp() {
    LOG.info("Starting ProgramStatusService");

    taskExecutorService.submit(new ProgramStatusSubscriberThread(
      cConf.get(Constants.Scheduler.STREAM_SIZE_EVENT_TOPIC)));
  }

  private class ProgramStatusSubscriberThread
      extends AbstractNotificationSubscriberService.NotificationSubscriberThread {

    ProgramStatusSubscriberThread(String topic) {
      super(topic);
    }

    @Override
    protected String loadMessageId() {
      return null;
    }

    @Override
    protected void processNotification(DatasetContext context, Notification notification) throws Exception {
      String programIdString = notification.getProperties().get(ProgramOptionConstants.PROGRAM_ID);
      long startTime = Long.valueOf(notification.getProperties().get(ProgramOptionConstants.LOGICAL_START_TIME));
      long endTime = Long.valueOf(notification.getProperties().get(ProgramOptionConstants.END_TIME));
      String programRunId = notification.getProperties().get(ProgramOptionConstants.RUN_ID);
      String twillRunId = notification.getProperties().get(ProgramOptionConstants.TWILL_RUN_ID);
      String userOverridesString = notification.getProperties().get(ProgramOptionConstants.USER_OVERRIDES);
      String systemOverridesString = notification.getProperties().get(ProgramOptionConstants.SYSTEM_OVERRIDES);
      String programStatusString = notification.getProperties().get(ProgramOptionConstants.PROGRAM_STATUS);
      String basicThrowableString = notification.getProperties().get("error");
      ProgramRunStatus programRunStatus = null;
      try {
        programRunStatus = ProgramRunStatus.valueOf(programStatusString);
      } catch (IllegalArgumentException e) {
        LOG.warn("Invalid program status {} passed for programId {}", programStatusString, programIdString, e);
        // Fall through, let the thread return normally
      }

      // Ignore notifications which specify an invalid ProgramId, RunId, or ProgramStatus
      if (programIdString == null || programRunId == null || programRunStatus == null) {
        return;
      }

      ProgramId programId = ProgramId.fromString(programIdString);

      switch(programRunStatus) {
        case RUNNING:
          Map<String, String> userOverrides = GSON.fromJson(userOverridesString, STRING_STRING_MAP);
          Map<String, String> systemOverrides = GSON.fromJson(systemOverridesString, STRING_STRING_MAP);
          runtimeStore.setStart(programId, programRunId, startTime, twillRunId, userOverrides, systemOverrides);
          break;
        case COMPLETED:
        case SUSPENDED:
        case KILLED:
          runtimeStore.setStop(programId, programRunId, endTime, programRunStatus);
          break;
        case FAILED:
          BasicThrowable cause = GSON.fromJson(basicThrowableString, BasicThrowable.class);
          runtimeStore.setStop(programId, programRunId, endTime, ProgramRunStatus.FAILED, cause);
          break;
        default:
          throw new IllegalArgumentException("Well this is not good");
      }
    }
  }
}

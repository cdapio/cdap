/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.report.main;

import co.cask.cdap.api.artifact.ArtifactId;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.messaging.Message;
import co.cask.cdap.report.proto.Filter;
import co.cask.cdap.report.proto.FilterDeserializer;
import co.cask.cdap.report.util.Constants;
import com.google.common.primitives.Longs;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Contains utility methods for finding latest message id to process from
 * and also construct {@link ProgramRunInfo} from a given Message.
 */
public final class MessageUtil {
  private static final Logger LOG = LoggerFactory.getLogger(MessageUtil.class);
  private static final SampledLogging SAMPLED_LOGGING = new SampledLogging(LOG, 100);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Filter.class, new FilterDeserializer())
    .create();
  private static final Type MAP_TYPE =
    new TypeToken<Map<String, String>>() { }.getType();

  private MessageUtil() {

  }

  /**
   * For each of the namespace at the base location, find the latest file under that namespace and from
   * the last written record, find the latest messageId for that namespace,
   * return the overall max messageId across the namespaces, if no namespace is found, return null
   * @param baseLocation base location for all the namespaces under reporting fileset*
   * @return messageId
   * @throws InterruptedException
   */
  @Nullable
  public static String findMessageId(Location baseLocation) throws InterruptedException {
    List<Location> namespaces = listLocationsWithRetry(baseLocation);
    byte[] messageId = Bytes.EMPTY_BYTE_ARRAY;
    String resultMessageId = null;
    for (Location namespaceLocation : namespaces) {
      // TODO keep trying with earlier file, if the latest file is empty
      Optional<Location> latest = findLatestFileLocation(namespaceLocation);
      if (latest.isPresent()) {
        String messageIdString = getLatestMessageIdFromFile(latest.get());
        if (Bytes.compareTo(Bytes.fromHexString(messageIdString), messageId) > 0) {
          messageId = Bytes.fromHexString(messageIdString);
          resultMessageId = messageIdString;
        }
      }
    }
    return resultMessageId;
  }

  /**
   * Based on the {@link Message} and its ProgramStatus,
   * construct by setting the fields of {@link ProgramRunInfo} and return that.
   * @param message TMS message
   * @return {@link ProgramRunInfo}
   */
  public static ProgramRunInfo constructAndGetProgramRunIdFields(Message message) {
    Notification notification = GSON.fromJson(message.getPayloadAsString(), Notification.class);
    ProgramRunInfo programRunInfo =
      GSON.fromJson(notification.getProperties().get(Constants.Notification.PROGRAM_RUN_ID), ProgramRunInfo.class);
    programRunInfo.setMessageId(message.getId());

    String programStatus = notification.getProperties().get(Constants.Notification.PROGRAM_STATUS);
    programRunInfo.setStatus(programStatus);

    switch (programStatus) {
      case Constants.Notification.Status.STARTING:
        programRunInfo.setTime(Long.parseLong(notification.getProperties().get(Constants.Notification.START_TIME)));
        ArtifactId artifactId =
          GSON.fromJson(notification.getProperties().get(Constants.Notification.ARTIFACT_ID), ArtifactId.class);
        Map<String, String> userArguments =
          GSON.fromJson(notification.getProperties().get(Constants.Notification.USER_OVERRIDES), MAP_TYPE);

        String principal = notification.getProperties().get(Constants.Notification.PRINCIPAL);

        ProgramStartInfo programStartInfo = new ProgramStartInfo(userArguments, artifactId, principal);
        programRunInfo.setStartInfo(programStartInfo);
        break;
      case Constants.Notification.Status.RUNNING:
        programRunInfo.setTime(
          Long.parseLong(notification.getProperties().get(Constants.Notification.LOGICAL_START_TIME)));
        break;
      case Constants.Notification.Status.KILLED:
      case Constants.Notification.Status.COMPLETED:
      case Constants.Notification.Status.FAILED:
        programRunInfo.setTime(
          Long.parseLong(notification.getProperties().get(Constants.Notification.END_TIME)));
        break;
      case Constants.Notification.Status.SUSPENDED:
        programRunInfo.setTime(
          Long.parseLong(notification.getProperties().get(Constants.Notification.SUSPEND_TIME)));
        break;
      case Constants.Notification.Status.RESUMING:
        programRunInfo.setTime(
          Long.parseLong(notification.getProperties().get(Constants.Notification.RESUME_TIME)));
        break;
    }
    return programRunInfo;
  }


  private static List<Location> listLocationsWithRetry(Location location) throws InterruptedException {
    boolean success = false;
    while (!success) {
      try {
        return location.list();
      } catch (IOException e) {
        SAMPLED_LOGGING.logWarning(
          String.format("Exception while listing the location list at %s ", location.toURI()), e);
        TimeUnit.MILLISECONDS.sleep(10);
      }
    }
    return Collections.emptyList();
  }

  private static String getLatestMessageIdFromFile(Location latest) throws InterruptedException {
    boolean success = false;
    String messageId = null;
    while (!success) {
      try {
        DataFileReader<GenericRecord> dataFileReader =
          new DataFileReader<>(new File(latest.toURI()),
                                            new GenericDatumReader<>(ProgramRunIdFieldsSerializer.SCHEMA));
        // able to get data file reader without any IOException
        success = true;
        while (dataFileReader.hasNext()) {
          GenericRecord record = dataFileReader.next();
          messageId = record.get(Constants.MESSAGE_ID).toString();
        }
      } catch (IOException e) {
        SAMPLED_LOGGING.logWarning(
          String.format("IOException while trying to create a DataFileReader for the location %s ",
                        latest.toURI()), e);
        TimeUnit.MILLISECONDS.sleep(10);
      }
    }
    return messageId;
  }

  @Nullable
  private static Optional<Location> findLatestFileLocation(Location namespaceLocation) throws InterruptedException {
    List<Location> latestLocations = new ArrayList();
    latestLocations.addAll(listLocationsWithRetry(namespaceLocation));
    return latestLocations.stream().max((Location o1, Location o2) -> {
      String fileName1 = o1.getName();
      // format is <event-ts>-<creation-ts>.avro, we parse and get the creation-ts
      long creatingTime1 = Long.parseLong(fileName1.substring(0, fileName1.indexOf(".avro")).split("-")[1]);
      String fileName2 = o2.getName();
      long creatingTime2 = Long.parseLong(fileName2.substring(0, fileName2.indexOf(".avro")).split("-")[1]);
      // latest file will be at the end in the list
      return Longs.compare(creatingTime1, creatingTime2);
    });
  }
}

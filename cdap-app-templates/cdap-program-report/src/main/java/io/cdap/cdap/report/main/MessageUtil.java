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

package io.cdap.cdap.report.main;

import com.google.common.primitives.Longs;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.messaging.Message;
import io.cdap.cdap.report.proto.Filter;
import io.cdap.cdap.report.proto.FilterCodec;
import io.cdap.cdap.report.util.Constants;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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
    .registerTypeAdapter(Filter.class, new FilterCodec())
    .create();
  private static final Type MAP_TYPE =
    new TypeToken<Map<String, String>>() { }.getType();
  // Number of 100ns intervals since 15 October 1582 00:00:000000000 until UNIX epoch
  private static final long NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01b21dd213814000L;
  // Multiplier to convert millisecond into 100ns
  private static final long HUNDRED_NANO_MULTIPLIER = 10000;
  private static final String CDAP_VERSION = "cdap.version";

  private MessageUtil() {

  }

  /**
   * For each of the namespace at the base location, find the latest file under that namespace and from
   * the last written record, find the latest messageId for that namespace,
   * return the overall max messageId across the namespaces, if no namespace is found, return null
   * @param baseLocation base location for all the namespaces under reporting fileset
   * @return messageId
   * @throws InterruptedException
   */
  @Nullable
  public static String findMessageId(Location baseLocation) throws InterruptedException {
    List<Location> namespaces = listLocationsWithRetry(baseLocation);
    byte[] messageId = Bytes.EMPTY_BYTE_ARRAY;
    String resultMessageId = null;
    for (Location namespaceLocation : namespaces) {
      List<Location> nsLocationsSorted = getLocationsSorted(namespaceLocation);
      if (!nsLocationsSorted.isEmpty()) {
        String messageIdString = getLatestMessageId(nsLocationsSorted);
        if (messageIdString != null) {
          if (Bytes.compareTo(Bytes.fromHexString(messageIdString), messageId) > 0) {
            messageId = Bytes.fromHexString(messageIdString);
            resultMessageId = messageIdString;
          }
        }
      }
    }
    return resultMessageId;
  }

  /**
   * Deserialize TMS message into {@link Notification}
   * @param message
   * @return Notification
   */
  public static Notification messageToNotification(Message message) {
    return message.decodePayload(r -> GSON.fromJson(r, Notification.class));
  }

  /**
   * CDAP_VERSION key was added only on CDAP 5.0 and Reporting app only works after CDAP 5.0 and we want
   * to skip pre 5.0 records. This method checks and return true if the key is present.
   * @param notification
   * @return true if CDAP_VERSION key is present in Notification properties
   */
  public static boolean isCDAPVersionCompatible(Notification notification) {
    return notification.getProperties().containsKey(CDAP_VERSION);
  }

  /**
   * Based on the {@link Message} and its ProgramStatus,
   * construct by setting the fields of {@link ProgramRunInfo} and return that.
   * @param message TMS message
   * @param notification
   * @return {@link ProgramRunInfo}
   */
  public static ProgramRunInfo constructAndGetProgramRunInfo(Message message,
                                                             Notification notification) throws IOException {
    ProgramRunInfo programRunInfo =
      GSON.fromJson(notification.getProperties().get(Constants.Notification.PROGRAM_RUN_ID), ProgramRunInfo.class);
    programRunInfo.setMessageId(message.getId());

    String programStatus = notification.getProperties().get(Constants.Notification.PROGRAM_STATUS);
    programRunInfo.setStatus(programStatus);

    switch (programStatus) {
      case Constants.Notification.Status.STARTING:
        long startTime = getTime(programRunInfo.getRun(), TimeUnit.MILLISECONDS);
        programRunInfo.setTime(startTime);
        ProgramDescriptor programDescriptor =
          GSON.fromJson(notification.getProperties().get(Constants.Notification.PROGRAM_DESCRIPTOR),
                        ProgramDescriptor.class);
        ArtifactId artifactId = programDescriptor.getArtifactId();

        Map<String, String> systemArguments =
          GSON.fromJson(notification.getProperties().get(Constants.Notification.SYSTEM_OVERRIDES), MAP_TYPE);

        Map<String, String> userArguments =
          GSON.fromJson(notification.getProperties().get(Constants.Notification.USER_OVERRIDES), MAP_TYPE);

        String principal = systemArguments.get(Constants.Notification.PRINCIPAL);
        if (principal != null) {
          principal = new KerberosName(principal).getShortName();
        }
        ProgramStartInfo programStartInfo = new ProgramStartInfo(userArguments, artifactId, principal, systemArguments);
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

  /**
   * Copied from RunIds in cdap-common
   * @return time from the UUID if it is a time-based UUID, -1 otherwise.
   */
  private static long getTime(String runId, TimeUnit timeUnit) {
    UUID uuid = UUID.fromString(runId);
    if (uuid.version() == 1 && uuid.variant() == 2) {
      long timeInMilliseconds = (uuid.timestamp() - NUM_100NS_INTERVALS_SINCE_UUID_EPOCH) / HUNDRED_NANO_MULTIPLIER;
      return timeUnit.convert(timeInMilliseconds, TimeUnit.MILLISECONDS);
    }
    return -1;
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

  private static String getLatestMessageId(List<Location> locationsTimeSorted) throws InterruptedException {
    String messageId = null;
    int index = locationsTimeSorted.size() - 1;
    // there's a possibility that the latest file could be empty, so we go through the list of locations in
    // descending time order and return the messageId from the latest file which has content.
    while (index >= 0) {
      Location latestLocation = locationsTimeSorted.get(index);
      try {
        DataFileStream<GenericRecord> dataFileStream =
          new DataFileStream<>(latestLocation.getInputStream(),
                               new GenericDatumReader<>(ProgramRunInfoSerializer.SCHEMA));
        //TODO - efficiently skip using sync points CDAP-13400
        while (dataFileStream.hasNext()) {
          GenericRecord record = dataFileStream.next();
          messageId = record.get(Constants.MESSAGE_ID).toString();
        }
        if (messageId != null) {
          return messageId;
        }
        index--;
      } catch (IOException e) {
        SAMPLED_LOGGING.logWarning(
          String.format("IOException while trying to create a DataFileReader for the location %s ",
                        latestLocation.toURI()), e);
        TimeUnit.MILLISECONDS.sleep(10);
      }
    }
    return messageId;
  }

  @Nullable
  private static List<Location> getLocationsSorted(Location namespaceLocation) throws InterruptedException {
    List<Location> nsLocations = new ArrayList();
    nsLocations.addAll(listLocationsWithRetry(namespaceLocation));
    nsLocations.sort((Location o1, Location o2) -> {
      String fileName1 = o1.getName();
      // format is <event-ts>-<creation-ts>.avro, we parse and get the creation-ts
      long creatingTime1 = Long.parseLong(fileName1.substring(0, fileName1.indexOf(".avro")).split("-")[1]);
      String fileName2 = o2.getName();
      long creatingTime2 = Long.parseLong(fileName2.substring(0, fileName2.indexOf(".avro")).split("-")[1]);
      // latest file will be at the end in the list
      return Longs.compare(creatingTime1, creatingTime2);
    });
    return nsLocations;
  }
}

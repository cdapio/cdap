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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * utility methods
 */
public final class MessageUtil {
  private static final Logger LOG = LoggerFactory.getLogger(MessageUtil.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Filter.class, new FilterDeserializer())
    .create();
  private static final Type MAP_TYPE =
    new co.cask.cdap.internal.guava.reflect.TypeToken<Map<String, String>>() { }.getType();

  private MessageUtil() {

  }

  public static String findMessageId(Location baseLocation) throws IOException {
    List<Location> namespaces = baseLocation.list();
    byte[] messageId = Bytes.EMPTY_BYTE_ARRAY;
    String resultMessageId = null;
    for (Location namespaceLocation : namespaces) {
      // TODO keep trying with earlier file, if the latest file is empty
      Location latest = findLatestFileLocation(namespaceLocation);
      if (latest != null) {
        String messageString = getLatestMessageIdFromFile(latest);
        if (Bytes.compareTo(Bytes.fromHexString(messageString), messageId) > 0) {
          messageId = Bytes.fromHexString(messageString);
          resultMessageId = messageString;
        }
      }
    }
    return resultMessageId;
  }


  public static ProgramRunIdFields constructAndGetProgramRunIdFields(Message message) {
    Notification notification = GSON.fromJson(message.getPayloadAsString(), Notification.class);
    LOG.trace("Get notification: {}", notification);
    ProgramRunIdFields programRunIdFields =
      GSON.fromJson(notification.getProperties().get("programRunId"), ProgramRunIdFields.class);
    programRunIdFields.setMessageId(message.getId());

    String programStatus = notification.getProperties().get("programStatus");
    programRunIdFields.setStatus(programStatus);


    switch (programStatus) {
      case "STARTING":
        programRunIdFields.setTime(Long.parseLong(notification.getProperties().get("startTime")));
        ArtifactId artifactId = GSON.fromJson(notification.getProperties().get("artifactId"), ArtifactId.class);
        Map<String, String> userArguments =
          GSON.fromJson(notification.getProperties().get("userOverrides"), MAP_TYPE);
        Map<String, String> systemArguments =
          GSON.fromJson(notification.getProperties().get("systemOverrides"), MAP_TYPE);
        systemArguments.putAll(userArguments);
        String principal = notification.getProperties().get("principal");

        ProgramStartInfo programStartInfo = new ProgramStartInfo(systemArguments, artifactId, principal);
        programRunIdFields.setStartInfo(programStartInfo);
        break;
      case "RUNNING":
        programRunIdFields.setTime(Long.parseLong(notification.getProperties().get("logical.start.time")));
        break;
      case "KILLED":
      case "COMPLETED":
      case "FAILED":
        programRunIdFields.setTime(Long.parseLong(notification.getProperties().get("endTime")));
        break;
      case "SUSPENDED":
        programRunIdFields.setTime(Long.parseLong(notification.getProperties().get("suspendTime")));
        break;
      case "RESUMING":
        programRunIdFields.setTime(Long.parseLong(notification.getProperties().get("resumeTime")));
        break;
    }
    return programRunIdFields;
  }

  private static String getLatestMessageIdFromFile(Location latest) throws IOException {
    DataFileReader<GenericRecord> dataFileReader =
      new DataFileReader<GenericRecord>(new File(latest.toURI()),
                                        new GenericDatumReader<GenericRecord>(ProgramRunIdFieldsSerializer.SCHEMA));
    String messageId = null;
    while (dataFileReader.hasNext()) {
      GenericRecord record = dataFileReader.next();
      messageId = record.get(Constants.MESSAGE_ID).toString();
    }
    return messageId;
  }

  @Nullable
  private static Location findLatestFileLocation(Location namespaceLocation) throws IOException {
    List<Location> latestLocations = new ArrayList();
    latestLocations.addAll(namespaceLocation.list());
    latestLocations.sort(new Comparator<Location>() {
      @Override
      public int compare(Location o1, Location o2) {
        String fileName1 = o1.getName();
        long creatingTime1 = Long.parseLong(fileName1.substring(0, fileName1.indexOf(".avro")).split("-")[1]);
        String fileName2 = o2.getName();
        long creatingTime2 = Long.parseLong(fileName2.substring(0, fileName2.indexOf(".avro")).split("-")[1]);
        // latest file should be the first in the list
        return Longs.compare(creatingTime2, creatingTime1);
      }
    });
    if (latestLocations.isEmpty()) {
      return null;
    }
    return latestLocations.get(0);
  }
}

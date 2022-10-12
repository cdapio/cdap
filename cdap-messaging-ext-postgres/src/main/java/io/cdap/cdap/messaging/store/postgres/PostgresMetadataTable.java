/*
 * Copyright © 2021-2022 Cask Data, Inc.
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

package io.cdap.cdap.messaging.store.postgres;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.messaging.TopicAlreadyExistsException;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.messaging.TopicMetadata;
import io.cdap.cdap.messaging.data.TopicId;
import io.cdap.cdap.messaging.store.MetadataTable;

import java.io.IOException;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;

public class PostgresMetadataTable implements MetadataTable {
  public static final String TOPIC_TABLE = "topic_metadata";
  private static final Gson GSON = new Gson();
  private static final Type MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private final DataSource dataSource;

  PostgresMetadataTable(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  @Override
  public TopicMetadata getMetadata(TopicId topicId) throws TopicNotFoundException, IOException {
    try (Connection connection = dataSource.getConnection()) {
      String existsSql = "select properties from " + TOPIC_TABLE + " where topic =" + topicId.getTopic();
      Statement stmt = connection.createStatement();
      ResultSet rs = stmt.executeQuery(existsSql);
      if (rs.next()) {
        byte[] value = rs.getBytes(0);
        Map<String, String> properties = GSON.fromJson(Bytes.toString(value), MAP_TYPE);
        return new TopicMetadata(topicId, properties);
      } else {
        throw new TopicNotFoundException(topicId.getNamespace(), topicId.getTopic());
      }

    } catch (SQLException e) {
      throw new IOException("Error in topic creation", e);
    }
  }

  @Override
  public void createTopic(TopicMetadata topicMetadata) throws TopicAlreadyExistsException, IOException {
    TopicId topicId = topicMetadata.getTopicId();
    try (Connection connection = dataSource.getConnection()) {
      String existsSql = "select count(*) from " + TOPIC_TABLE + " where topic =" + topicId.getTopic();
      Statement stmt = connection.createStatement();
      ResultSet rs = stmt.executeQuery(existsSql);
      if (rs.next()) {
        throw new TopicAlreadyExistsException(topicId.getNamespace(), topicId.getTopic());
      }

      String insertSql = String.format("insert into %s (namespace, topic) values ('%s', '%s')", TOPIC_TABLE,
                                       topicId.getNamespace(), topicId.getTopic());
      stmt = connection.createStatement();
      stmt.executeUpdate(insertSql);
    } catch (SQLException e) {
      throw new IOException("Error in topic creation", e);
    }
  }

  @Override
  public void updateTopic(TopicMetadata topicMetadata) throws TopicNotFoundException, IOException {

  }

  @Override
  public void deleteTopic(TopicId topicId) throws TopicNotFoundException, IOException {

  }

  @Override
  public List<TopicId> listTopics(String namespaceId) throws IOException {
    return null;
  }

  @Override
  public List<TopicId> listTopics() throws IOException {
    return null;
  }

  @Override
  public void close() throws IOException {

  }
}

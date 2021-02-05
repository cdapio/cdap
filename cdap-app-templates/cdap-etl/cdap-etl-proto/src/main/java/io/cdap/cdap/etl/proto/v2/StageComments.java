/*
 * Copyright © 2021 Cask Data, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package io.cdap.cdap.etl.proto.v2;

import java.util.List;
import javax.annotation.Nullable;

/**
 * Class for holding comments for an ETL stage
 */
public class StageComments {
  private final List<Comment> list;

  public StageComments(List<Comment> list) {
    this.list = list;
  }

  /**
   * Class that represents a stage comment
   */
  protected class Comment {
    private final String content;
    private final long createDate;
    private final String user;

    public Comment(String content, long createDate, @Nullable String user) {
      this.content = content;
      this.createDate = createDate;
      this.user = user;
    }

    public String getContent() {
      return content;
    }

    public long getCreateDate() {
      return createDate;
    }

    public String getUser() {
      return user;
    }
  }
}


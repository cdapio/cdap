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

package co.cask.cdap.data.view;

import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.proto.ViewDetail;
import co.cask.cdap.proto.ViewSpecification;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.id.StreamViewId;

import java.util.List;

/**
 * Interface for storing stream views.
 */
public interface ViewStore {

  /**
   * Creates a view. Updates the view if it already exists.
   * @param viewId the view
   * @param config the view config
   * @return true if a new view was created
   */
  boolean createOrUpdate(StreamViewId viewId, ViewSpecification config);

  /**
   * @param viewId the view
   * @return true if the view exists
   */
  boolean exists(StreamViewId viewId);

  /**
   * Deletes a view.
   *
   * @param viewId the view
   */
  void delete(StreamViewId viewId) throws NotFoundException;

  /**
   * @param streamId the stream
   * @return list of view IDs for a stream
   */
  List<StreamViewId> list(StreamId streamId);

  /**
   * @param viewId the view
   * @return the details of a view
   */
  ViewDetail get(StreamViewId viewId) throws NotFoundException;
}

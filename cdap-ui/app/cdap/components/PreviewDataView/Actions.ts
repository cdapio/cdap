/*
 * Copyright © 2020 Cask Data, Inc.
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

import { Map, List } from 'immutable';
import { IConnection } from 'components/PipelineContextMenu/PipelineTypes';
import { MyPreviewApi } from 'api/preview';
import { getCurrentNamespace } from 'services/NamespaceStore';

type IAdjancencyMap = Map<string, List<string>>;

function getAdjacencyMap(connections: IConnection[]): IAdjancencyMap {
  let adjacencyMap: IAdjancencyMap = Map();
  if (!Array.isArray(connections)) {
    return adjacencyMap;
  }
  connections.forEach((conn) => {
    const alreadyExistingConnections = adjacencyMap.get(conn.from);
    if (Array.isArray(alreadyExistingConnections)) {
      adjacencyMap = adjacencyMap.set(
        conn.from,
        List<string>(alreadyExistingConnections.concat([conn.to]))
      );
    } else {
      adjacencyMap = adjacencyMap.set(conn.from, List([conn.to]));
    }
  });
  return adjacencyMap;
}

function fetchPreview({ previewId, stages, connections, currentStage }) {
  const adjacencyMap: IAdjancencyMap = getAdjacencyMap(connections);
  // let postBody = [];
  // let previousStageNames = Object.keys(adjacencyMap).filter(key => adjacencyMap[key].indexOf(this.state.node.plugin.label) !== -1);
  const previousStageNames1 = [];
  adjacencyMap.map((value, key) => {
    if (value.includes(currentStage)) {
      previousStageNames1.push(key);
    }
  });
  // MyPreviewApi.getStageData({
  //   namespace: getCurrentNamespace(),
  //   previewId,
  // })
  console.log(previousStageNames1);
  console.log(adjacencyMap);
}

export { fetchPreview, getAdjacencyMap };

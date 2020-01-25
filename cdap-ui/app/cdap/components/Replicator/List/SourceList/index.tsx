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

import * as React from 'react';
import { MyReplicatorApi } from 'api/replicator';
import { getCurrentNamespace } from 'services/NamespaceStore';
import { PluginType } from 'components/Replicator/constants';
import { Link } from 'react-router-dom';

const SourceList: React.FC = () => {
  const [sources, setSources] = React.useState([]);

  React.useEffect(() => {
    const params = {
      namespace: getCurrentNamespace(),
      pluginType: PluginType.source,
    };

    MyReplicatorApi.getPlugins(params).subscribe((res) => {
      // TODO: aggregate versions
      setSources(res);
    });
  }, []);

  return (
    <div>
      <h2>Sources</h2>

      <ul>
        {sources.map((source) => {
          const { name: artifactName, version, scope } = source.artifact;

          return (
            <li key={source.name}>
              <Link
                to={`/ns/${getCurrentNamespace()}/replicator/create/${artifactName}/${version}/${scope}/${
                  source.name
                }`}
              >
                {source.name}
              </Link>
            </li>
          );
        })}
      </ul>
    </div>
  );
};

export default SourceList;

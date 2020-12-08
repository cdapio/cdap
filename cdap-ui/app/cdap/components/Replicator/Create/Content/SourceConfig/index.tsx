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
import { createContextConnect, ICreateContext } from 'components/Replicator/Create';
import PluginConfig from 'components/Replicator/Create/Content/PluginConfig';
import { PluginType } from 'components/Replicator/constants';

const SourceConfigView: React.FC<ICreateContext> = ({
  sourcePluginInfo,
  sourcePluginWidget,
  sourceConfig,
  setSourcePluginInfo,
  setSourcePluginWidget,
  setSourceConfig,
}) => {
  return (
    <PluginConfig
      pluginType={PluginType.source}
      pluginInfo={sourcePluginInfo}
      pluginWidget={sourcePluginWidget}
      pluginConfig={sourceConfig}
      setPluginInfo={setSourcePluginInfo}
      setPluginWidget={setSourcePluginWidget}
      setPluginConfig={setSourceConfig}
    />
  );
};

const SourceConfig = createContextConnect(SourceConfigView);
export default SourceConfig;

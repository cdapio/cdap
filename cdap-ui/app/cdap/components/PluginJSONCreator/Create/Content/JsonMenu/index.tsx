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

import If from 'components/If';
import ClosedJsonMenu from 'components/PluginJSONCreator/Create/Content/JsonMenu/ClosedJsonMenu';
import JsonLiveViewer from 'components/PluginJSONCreator/Create/Content/JsonMenu/JsonLiveViewer';
import { CreateContext, createContextConnect, ICreateContext } from 'components/PluginJSONCreator/CreateContextConnect';
import * as React from 'react';

const JsonMenuView: React.FC<ICreateContext> = ({
  pluginName,
  pluginType,
  displayName,
  emitAlerts,
  emitErrors,
  configurationGroups,
  groupToInfo,
  groupToWidgets,
  widgetInfo,
  widgetToAttributes,
  jsonView,
  setJsonView,
}) => {
  return (
    <div>
      <If condition={jsonView}>
        <JsonLiveViewer
          pluginName={pluginName}
          pluginType={pluginType}
          displayName={displayName}
          emitAlerts={emitAlerts}
          emitErrors={emitErrors}
          configurationGroups={configurationGroups}
          groupToInfo={groupToInfo}
          groupToWidgets={groupToWidgets}
          widgetInfo={widgetInfo}
          widgetToAttributes={widgetToAttributes}
          jsonView={jsonView}
          setJsonView={setJsonView}
        />
      </If>
      <If condition={!jsonView}>
        <ClosedJsonMenu
          pluginName={pluginName}
          pluginType={pluginType}
          displayName={displayName}
          emitAlerts={emitAlerts}
          emitErrors={emitErrors}
          configurationGroups={configurationGroups}
          groupToInfo={groupToInfo}
          groupToWidgets={groupToWidgets}
          widgetInfo={widgetInfo}
          widgetToAttributes={widgetToAttributes}
          jsonView={jsonView}
          setJsonView={setJsonView}
        />
      </If>
    </div>
  );
};

const JsonMenu = createContextConnect(CreateContext, JsonMenuView);
export default JsonMenu;

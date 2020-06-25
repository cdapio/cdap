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
import LiveConfigurationGroup from 'components/PluginJSONCreator/Create/PluginJSONMenu/LiveViewMenu/LiveConfigurationGroup';
import LiveJSON from 'components/PluginJSONCreator/Create/PluginJSONMenu/LiveViewMenu/LiveJSON';
import LiveViewTopPanel from 'components/PluginJSONCreator/Create/PluginJSONMenu/LiveViewMenu/LiveViewTopPanel';
import * as React from 'react';

export enum LiveViewMode {
  JSONView = 'JSON_VIEW',
  ConfigurationGroupsView = 'CONFIGURATION_GROUPS',
}

interface ILiveViewMenuProps {
  JSONOutput: any;
  collapseLiveView: () => void;
  onDownloadClick: () => void;
  populateImportResults: (filename: string, fileContent: string) => void;
  JSONFilename: string;
  downloadDisabled: boolean;
}

const LiveViewMenu: React.FC<ILiveViewMenuProps> = ({
  JSONOutput,
  collapseLiveView,
  onDownloadClick,
  populateImportResults,
  JSONFilename,
  downloadDisabled,
}) => {
  const [liveViewMode, setLiveViewMode] = React.useState(LiveViewMode.JSONView);

  return (
    <div>
      <LiveViewTopPanel
        populateImportResults={populateImportResults}
        downloadDisabled={downloadDisabled}
        onDownloadClick={onDownloadClick}
        JSONFilename={JSONFilename}
        liveViewMode={liveViewMode}
        setLiveViewMode={setLiveViewMode}
        collapseLiveView={collapseLiveView}
      />
      <If condition={liveViewMode === LiveViewMode.JSONView}>
        <LiveJSON JSONOutput={JSONOutput} />
      </If>
      <If condition={liveViewMode === LiveViewMode.ConfigurationGroupsView}>
        <LiveConfigurationGroup JSONOutput={JSONOutput} />
      </If>
    </div>
  );
};

export default LiveViewMenu;

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

import Heading, { HeadingTypes } from 'components/Heading';
import FilterCollection from 'components/PluginJSONCreator/Create/Content/Filters/FilterCollection';
import JsonMenu from 'components/PluginJSONCreator/Create/Content/JsonMenu';
import StepButtons from 'components/PluginJSONCreator/Create/Content/StepButtons';
import {
  CreateContext,
  createContextConnect,
  ICreateContext,
} from 'components/PluginJSONCreator/CreateContextConnect';
import * as React from 'react';

const FiltersView: React.FC<ICreateContext> = ({
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
  filters,
  setFilters,
  filterToName,
  setFilterToName,
  filterToCondition,
  setFilterToCondition,
  filterToShowList,
  setFilterToShowList,
  showToInfo,
  setShowToInfo,
  liveView,
  setLiveView,
  outputName,
  setPluginState,
  JSONStatus,
  setJSONStatus,
}) => {
  const [localFilters, setLocalFilters] = React.useState(filters);
  const [localFilterToName, setLocalFilterToName] = React.useState(filterToName);
  const [localFilterToCondition, setLocalFilterToCondition] = React.useState(filterToCondition);
  const [localFilterToShowList, setLocalFilterToShowList] = React.useState(filterToShowList);
  const [localShowToInfo, setLocalShowToInfo] = React.useState(showToInfo);

  function saveAllResults() {
    setFilters(localFilters);
    setFilterToName(localFilterToName);
    setFilterToCondition(localFilterToCondition);
    setFilterToShowList(localFilterToShowList);
    setShowToInfo(localShowToInfo);
  }

  return (
    <div>
      <JsonMenu
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
        filters={localFilters}
        filterToName={localFilterToName}
        filterToCondition={localFilterToCondition}
        filterToShowList={localFilterToShowList}
        showToInfo={localShowToInfo}
        liveView={liveView}
        setLiveView={setLiveView}
        outputName={outputName}
        setPluginState={setPluginState}
        JSONStatus={JSONStatus}
        setJSONStatus={setJSONStatus}
      />
      <Heading type={HeadingTypes.h3} label="Filters" />
      <FilterCollection
        filters={localFilters}
        setFilters={setLocalFilters}
        filterToName={localFilterToName}
        setFilterToName={setLocalFilterToName}
        filterToCondition={localFilterToCondition}
        setFilterToCondition={setLocalFilterToCondition}
        filterToShowList={localFilterToShowList}
        setFilterToShowList={setLocalFilterToShowList}
        showToInfo={localShowToInfo}
        setShowToInfo={setLocalShowToInfo}
        widgetInfo={widgetInfo}
      />
      <StepButtons nextDisabled={false} onPrevious={saveAllResults} onNext={saveAllResults} />
    </div>
  );
};

const Filters = createContextConnect(CreateContext, FiltersView);
export default Filters;

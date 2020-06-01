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

<<<<<<< HEAD
=======
import { WithStyles } from '@material-ui/core/styles/withStyles';
import { styles } from 'components/AbstractWidget/RadioGroupWidget';
>>>>>>> ceedd81608d... [CDAP-16869] Create a page for outputs configuration (plugin JSON creator)
import Heading, { HeadingTypes } from 'components/Heading';
import JsonMenu from 'components/PluginJSONCreator/Create/Content/JsonMenu';
import PluginInput from 'components/PluginJSONCreator/Create/Content/PluginInput';
import StepButtons from 'components/PluginJSONCreator/Create/Content/StepButtons';
import {
  CreateContext,
  createContextConnect,
  ICreateContext,
} from 'components/PluginJSONCreator/CreateContextConnect';
import * as React from 'react';

<<<<<<< HEAD
const OutputsView: React.FC<ICreateContext> = ({
=======
const OutputsView: React.FC<ICreateContext & WithStyles<typeof styles>> = ({
  classes,
>>>>>>> ceedd81608d... [CDAP-16869] Create a page for outputs configuration (plugin JSON creator)
  pluginName,
  pluginType,
  displayName,
  emitAlerts,
  emitErrors,
  configurationGroups,
  groupToInfo,
  groupToWidgets,
<<<<<<< HEAD
  widgetInfo,
  widgetToAttributes,
  liveView,
  setLiveView,
  outputName,
  setOutputName,
  JSONStatus,
  setJSONStatus,
  setPluginState,
  filters,
  filterToName,
  filterToCondition,
  filterToShowList,
  showToInfo,
=======
  widgetToInfo,
  widgetToAttributes,
  jsonView,
  setJsonView,
  outputName,
  setOutputName,
>>>>>>> ceedd81608d... [CDAP-16869] Create a page for outputs configuration (plugin JSON creator)
}) => {
  const [localOutputName, setLocalOutputName] = React.useState(outputName);

  function saveAllResults() {
    setOutputName(localOutputName);
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
<<<<<<< HEAD
        widgetInfo={widgetInfo}
        widgetToAttributes={widgetToAttributes}
        liveView={liveView}
        setLiveView={setLiveView}
        outputName={localOutputName}
        JSONStatus={JSONStatus}
        setJSONStatus={setJSONStatus}
        setPluginState={setPluginState}
        filters={filters}
        filterToName={filterToName}
        filterToCondition={filterToCondition}
        filterToShowList={filterToShowList}
        showToInfo={showToInfo}
      />
      <Heading type={HeadingTypes.h3} label="Output" />
=======
        widgetToInfo={widgetToInfo}
        widgetToAttributes={widgetToAttributes}
        jsonView={jsonView}
        setJsonView={setJsonView}
        outputName={localOutputName}
      />
      <Heading type={HeadingTypes.h3} label="Outputs" />
>>>>>>> ceedd81608d... [CDAP-16869] Create a page for outputs configuration (plugin JSON creator)
      <br />
      <PluginInput
        widgetType={'textbox'}
        value={localOutputName}
<<<<<<< HEAD
        onChange={setLocalOutputName}
=======
        setValue={setLocalOutputName}
>>>>>>> ceedd81608d... [CDAP-16869] Create a page for outputs configuration (plugin JSON creator)
        label={'Output Name'}
        placeholder={'output name'}
        required={false}
      />
      <StepButtons nextDisabled={false} onPrevious={saveAllResults} onNext={saveAllResults} />
    </div>
  );
};

const Outputs = createContextConnect(CreateContext, OutputsView);
export default Outputs;

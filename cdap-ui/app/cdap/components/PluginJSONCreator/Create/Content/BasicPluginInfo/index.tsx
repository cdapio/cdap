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

import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';
import Heading, { HeadingTypes } from 'components/Heading';
import { PluginTypes } from 'components/PluginJSONCreator/constants';
import JsonMenu from 'components/PluginJSONCreator/Create/Content/JsonMenu';
import PluginInput from 'components/PluginJSONCreator/Create/Content/PluginInput';
import StepButtons from 'components/PluginJSONCreator/Create/Content/StepButtons';
import {
  CreateContext,
  createContextConnect,
  IBasicPluginInfo,
  ICreateContext,
} from 'components/PluginJSONCreator/CreateContextConnect';
import * as React from 'react';

const styles = (): StyleRules => {
  return {
    basicPluginInput: {
      marginTop: '30px',
      marginBottom: '30px',
    },
  };
};

const BasicPluginInfoView: React.FC<ICreateContext & WithStyles<typeof styles>> = ({
  classes,
  pluginName,
  pluginType,
  displayName,
  emitAlerts,
  emitErrors,
  setBasicPluginInfo,
  configurationGroups,
  groupToInfo,
  groupToWidgets,
  widgetInfo,
  widgetToAttributes,
  liveView,
  setLiveView,
  outputName,
  setPluginState,
  JSONStatus,
  setJSONStatus,
  filters,
  filterToName,
  filterToCondition,
  filterToShowList,
  showToInfo,
}) => {
  const [localPluginName, setLocalPluginName] = React.useState(pluginName);
  const [localPluginType, setLocalPluginType] = React.useState(pluginType);
  const [localDisplayName, setLocalDisplayName] = React.useState(displayName);
  const [localEmitAlerts, setLocalEmitAlerts] = React.useState(emitAlerts);
  const [localEmitErrors, setLocalEmitErrors] = React.useState(emitErrors);

  const requiredFilledOut =
    localPluginName.length > 0 && localPluginType.length > 0 && localDisplayName.length > 0;

  function handleNext() {
    setBasicPluginInfo({
      pluginName: localPluginName,
      pluginType: localPluginType,
      displayName: localDisplayName,
      emitAlerts: localEmitAlerts,
      emitErrors: localEmitErrors,
    } as IBasicPluginInfo);
  }

  return (
    <div>
      <JsonMenu
        pluginName={localPluginName}
        pluginType={localPluginType}
        displayName={localDisplayName}
        emitAlerts={localEmitAlerts}
        emitErrors={localEmitErrors}
        configurationGroups={configurationGroups}
        groupToInfo={groupToInfo}
        groupToWidgets={groupToWidgets}
        widgetInfo={widgetInfo}
        widgetToAttributes={widgetToAttributes}
        liveView={liveView}
        setLiveView={setLiveView}
        outputName={outputName}
        setPluginState={setPluginState}
        JSONStatus={JSONStatus}
        setJSONStatus={setJSONStatus}
        filters={filters}
        filterToName={filterToName}
        filterToCondition={filterToCondition}
        filterToShowList={filterToShowList}
        showToInfo={showToInfo}
      />
      <Heading type={HeadingTypes.h3} label="Basic Plugin Information" />
      <div className={classes.basicPluginInput}>
        <PluginInput
          widgetType={'textbox'}
          value={localPluginName}
          onChange={setLocalPluginName}
          label={'Plugin Name'}
          placeholder={'Select a Plugin Name'}
          required={true}
        />
      </div>
      <div className={classes.basicPluginInput}>
        <PluginInput
          widgetType={'select'}
          value={localPluginType}
          onChange={setLocalPluginType}
          label={'Plugin Type'}
          options={PluginTypes}
          required={true}
        />
      </div>
      <div className={classes.basicPluginInput}>
        <PluginInput
          widgetType={'textbox'}
          value={localDisplayName}
          onChange={setLocalDisplayName}
          label={'Display Name'}
          placeholder={'Select a Display Name'}
          required={true}
        />
      </div>
      <div className={classes.basicPluginInput}>
        <PluginInput
          widgetType={'toggle'}
          value={localEmitAlerts ? 'true' : 'false'}
          onChange={(val) => setLocalEmitAlerts(val === 'true')}
          label={'Emit Alerts?'}
        />
      </div>
      <div className={classes.basicPluginInput}>
        <PluginInput
          widgetType={'toggle'}
          value={localEmitErrors ? 'true' : 'false'}
          onChange={(val) => setLocalEmitErrors(val === 'true')}
          label={'Emit Errors?'}
        />
      </div>
      <StepButtons nextDisabled={!requiredFilledOut} onNext={handleNext} />
    </div>
  );
};

const StyledBasicPluginInfoView = withStyles(styles)(BasicPluginInfoView);
const BasicPluginInfo = createContextConnect(CreateContext, StyledBasicPluginInfoView);
export default BasicPluginInfo;

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
import { PLUGIN_TYPES } from 'components/PluginJSONCreator/constants';
import {
  createContextConnect,
  IBasicPluginInfo,
  ICreateContext,
} from 'components/PluginJSONCreator/Create';
import JsonLiveViewer from 'components/PluginJSONCreator/Create/Content/JsonLiveViewer';
import PluginInput from 'components/PluginJSONCreator/Create/Content/PluginInput';
import { PluginToggleInput } from 'components/PluginJSONCreator/Create/Content/PluginToggleInput';
import StepButtons from 'components/PluginJSONCreator/Create/Content/StepButtons';
import * as React from 'react';

const styles = (): StyleRules => {
  return {
    root: {
      padding: '30px 40px',
    },
    content: {
      width: '50%',
      maxWidth: '1000px',
      minWidth: '600px',
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
  configurationGroups,
  groupToInfo,
  groupToWidgets,
  widgetToInfo,
  widgetToAttributes,
  outputName,
  filters,
  filterToName,
  filterToCondition,
  filterToShowList,
  showToInfo,
  jsonView,
  setJsonView,
  setBasicPluginInfo,
}) => {
  const [localPluginName, setLocalPluginName] = React.useState(pluginName);
  const [localPluginType, setLocalPluginType] = React.useState(pluginType);
  const [localDisplayName, setLocalDisplayName] = React.useState(displayName);
  const [localEmitAlerts, setLocalEmitAlerts] = React.useState(emitAlerts);
  const [localEmitErrors, setLocalEmitErrors] = React.useState(emitErrors);

  const requiredFilledOut =
    localPluginName.length > 0 && localPluginType.length > 0 && localDisplayName.length > 0;

  function saveAllResults() {
    setBasicPluginInfo({
      pluginName: localPluginName,
      pluginType: localPluginType,
      displayName: localDisplayName,
      emitAlerts: localEmitAlerts,
      emitErrors: localEmitErrors,
    } as IBasicPluginInfo);
  }

  return (
    <div className={classes.root}>
      <JsonLiveViewer
        displayName={displayName}
        configurationGroups={configurationGroups}
        groupToInfo={groupToInfo}
        groupToWidgets={groupToWidgets}
        widgetToInfo={widgetToInfo}
        widgetToAttributes={widgetToAttributes}
        outputName={outputName}
        jsonView={jsonView}
        setJsonView={setJsonView}
        filters={filters}
        filterToName={filterToName}
        filterToCondition={filterToCondition}
        filterToShowList={filterToShowList}
        showToInfo={showToInfo}
      />
      <div className={classes.content}>
        <Heading type={HeadingTypes.h3} label="Basic Plugin Information" />
        <br />
        <PluginInput
          widgetType={'textbox'}
          value={localPluginName}
          setValue={setLocalPluginName}
          label={'Plugin Name'}
          placeholder={'Plugin Name'}
          required={true}
        />
        <br />
        <br />
        <PluginInput
          widgetType={'select'}
          value={localPluginType}
          setValue={setLocalPluginType}
          label={'Plugin Type'}
          options={PLUGIN_TYPES}
          required={true}
        />
        <br />
        <br />
        <PluginInput
          widgetType={'textbox'}
          value={localDisplayName}
          setValue={setLocalDisplayName}
          label={'Display Name'}
          placeholder={'Display Name'}
          required={true}
        />
        <br />
        <Heading type={HeadingTypes.h5} label="Emit Alerts?" />
        <PluginToggleInput
          label={'Emit Alerts?'}
          value={localEmitAlerts}
          setValue={setLocalEmitAlerts}
        />
        <br />
        <Heading type={HeadingTypes.h5} label="Emit Errors?" />
        <PluginToggleInput
          label={'Emit Errors?'}
          value={localEmitErrors}
          setValue={setLocalEmitErrors}
        />
      </div>
      <StepButtons
        nextDisabled={!requiredFilledOut}
        onPrevious={saveAllResults}
        onNext={saveAllResults}
      />
    </div>
  );
};

const StyledBasicPluginInfoView = withStyles(styles)(BasicPluginInfoView);
const BasicPluginInfo = createContextConnect(StyledBasicPluginInfoView);
export default BasicPluginInfo;

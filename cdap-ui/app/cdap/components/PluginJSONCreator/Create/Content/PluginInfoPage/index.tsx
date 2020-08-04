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

import Heading, { HeadingTypes } from 'components/Heading';
import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';

import PluginInput from 'components/PluginJSONCreator/Create/Content/PluginInput';
import { PluginTypes } from 'components/PluginJSONCreator/constants';
import StepButtons from 'components/PluginJSONCreator/Create/Content/StepButtons';
import { usePluginInfoState } from 'components/PluginJSONCreator/Create';

const styles = (theme): StyleRules => {
  return {
    basicPluginInput: {
      marginTop: theme.spacing(3),
      marginBottom: theme.spacing(3),
    },
  };
};

const PluginInfoPageView: React.FC<WithStyles<typeof styles>> = ({ classes }) => {
  const {
    pluginName,
    setPluginName,
    pluginType,
    setPluginType,
    displayName,
    setDisplayName,
    emitAlerts,
    setEmitAlerts,
    emitErrors,
    setEmitErrors,
  } = usePluginInfoState();

  const requiredFilledOut =
    pluginName.length > 0 && pluginType.length > 0 && displayName.length > 0;

  return (
    <div>
      <Heading type={HeadingTypes.h3} label="Plugin Information" />
      <div className={classes.basicPluginInput}>
        <PluginInput
          widgetType={'textbox'}
          value={pluginName}
          onChange={setPluginName}
          label={'Plugin name'}
          placeholder={'Select a Plugin Name'}
          required={true}
        />
      </div>
      <div className={classes.basicPluginInput}>
        <PluginInput
          widgetType={'select'}
          value={pluginType}
          onChange={setPluginType}
          label={'Plugin type'}
          options={PluginTypes}
          required={true}
        />
      </div>
      <div className={classes.basicPluginInput}>
        <PluginInput
          widgetType={'textbox'}
          value={displayName}
          onChange={setDisplayName}
          label={'Display name'}
          placeholder={'Select a Display Name'}
          required={true}
        />
      </div>
      <div className={classes.basicPluginInput}>
        <PluginInput
          widgetType={'toggle'}
          value={emitAlerts ? 'true' : 'false'}
          onChange={(val) => setEmitAlerts(val === 'true')}
          label={'Emit alerts?'}
        />
      </div>
      <div className={classes.basicPluginInput}>
        <PluginInput
          widgetType={'toggle'}
          value={emitErrors ? 'true' : 'false'}
          onChange={(val) => setEmitErrors(val === 'true')}
          label={'Emit errors?'}
        />
      </div>
      <StepButtons nextDisabled={!requiredFilledOut} />
    </div>
  );
};

const PluginInfoPage = withStyles(styles)(PluginInfoPageView);
export default PluginInfoPage;

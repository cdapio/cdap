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
import { usePluginInfoState } from 'components/PluginJSONCreator/Create';
import PluginInput from 'components/PluginJSONCreator/Create/Content/PluginInput';
import StepButtons from 'components/PluginJSONCreator/Create/Content/StepButtons';
import * as React from 'react';

const styles = (): StyleRules => {
  return {
    basicPluginInput: {
      marginTop: '30px',
      marginBottom: '30px',
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
          label={'Plugin Name'}
          placeholder={'Select a Plugin Name'}
          required={true}
        />
      </div>
      <div className={classes.basicPluginInput}>
        <PluginInput
          widgetType={'select'}
          value={pluginType}
          onChange={setPluginType}
          label={'Plugin Type'}
          options={PluginTypes}
          required={true}
        />
      </div>
      <div className={classes.basicPluginInput}>
        <PluginInput
          widgetType={'textbox'}
          value={displayName}
          onChange={setDisplayName}
          label={'Display Name'}
          placeholder={'Select a Display Name'}
          required={true}
        />
      </div>
      <div className={classes.basicPluginInput}>
        <PluginInput
          widgetType={'toggle'}
          value={emitAlerts ? 'true' : 'false'}
          onChange={(val) => setEmitAlerts(val === 'true')}
          label={'Emit Alerts?'}
        />
      </div>
      <div className={classes.basicPluginInput}>
        <PluginInput
          widgetType={'toggle'}
          value={emitErrors ? 'true' : 'false'}
          onChange={(val) => setEmitErrors(val === 'true')}
          label={'Emit Errors?'}
        />
      </div>
      <StepButtons nextDisabled={!requiredFilledOut} />
    </div>
  );
};

const PluginInfoPage = withStyles(styles)(PluginInfoPageView);
export default PluginInfoPage;

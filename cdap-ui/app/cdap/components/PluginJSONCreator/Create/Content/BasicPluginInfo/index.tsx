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
import ToggleSwitchWidget from 'components/AbstractWidget/ToggleSwitchWidget';
import WidgetWrapper from 'components/ConfigurationGroup/WidgetWrapper';
import Heading, { HeadingTypes } from 'components/Heading';
import { PluginTypes } from 'components/PluginJSONCreator/constants';
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

const PluginTextInput = ({ setValue, value, label }) => {
  const widget = {
    label,
    name: label,
    'widget-type': 'textbox',
    'widget-attributes': {
      placeholder: 'Select a ' + label,
    },
  };

  const property = {
    required: true,
    name: label,
  };

  return (
    <WidgetWrapper
      widgetProperty={widget}
      pluginProperty={property}
      value={value}
      onChange={setValue}
    />
  );
};

const PluginSelect = ({ setValue, value, label, options }) => {
  const widget = {
    label,
    name: label,
    'widget-type': 'select',
    'widget-attributes': {
      options,
      default: options[0],
    },
  };

  const property = {
    required: true,
    name: label,
  };

  return (
    <WidgetWrapper
      widgetProperty={widget}
      pluginProperty={property}
      value={value}
      onChange={setValue}
    />
  );
};

const PluginToggle = ({ setValue, value, label }) => {
  const widget = {
    label,
    name: label,
    'widget-type': 'toggle',
    'widget-attributes': {
      default: value ? 'true' : 'false',
      on: {
        value: 'true',
        label: 'True',
      },
      off: {
        value: 'false',
        label: 'False',
      },
    },
  };

  return <ToggleSwitchWidget widgetProps={widget} value={value} onChange={setValue} />;
};

const BasicPluginInfoView: React.FC<ICreateContext & WithStyles<typeof styles>> = ({
  classes,
  pluginName,
  pluginType,
  displayName,
  emitAlerts,
  emitErrors,
  setBasicPluginInfo,
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
    <div className={classes.root}>
      <div className={classes.content}>
        <Heading type={HeadingTypes.h3} label="Basic Plugin Information" />
        <br />
        <PluginTextInput
          label={'Plugin Name'}
          value={localPluginName}
          setValue={setLocalPluginName}
        />
        <br />
        <br />
        <PluginSelect
          label={'Plugin Type'}
          options={PluginTypes}
          value={localPluginType}
          setValue={setLocalPluginType}
        />
        <br />
        <br />
        <PluginTextInput
          label={'Display Name'}
          value={localDisplayName}
          setValue={setLocalDisplayName}
        />
        <br />
        <Heading type={HeadingTypes.h5} label="Emit Alerts?" />
        <PluginToggle
          label={'Emit Alerts?'}
          value={localEmitAlerts}
          setValue={setLocalEmitAlerts}
        />
        <br />
        <Heading type={HeadingTypes.h5} label="Emit Errors?" />
        <PluginToggle
          label={'Emit Errors?'}
          value={localEmitErrors}
          setValue={setLocalEmitErrors}
        />
      </div>
      <StepButtons nextDisabled={!requiredFilledOut} onNext={handleNext} />
    </div>
  );
};

const StyledBasicPluginInfoView = withStyles(styles)(BasicPluginInfoView);
const BasicPluginInfo = createContextConnect(CreateContext, StyledBasicPluginInfoView);
export default BasicPluginInfo;

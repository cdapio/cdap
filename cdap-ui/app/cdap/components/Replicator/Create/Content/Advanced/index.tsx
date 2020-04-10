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
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import { createContextConnect, ICreateContext } from 'components/Replicator/Create';
import StepButtons from 'components/Replicator/Create/Content/StepButtons';
import WidgetWrapper from 'components/ConfigurationGroup/WidgetWrapper';
import Heading, { HeadingTypes } from 'components/Heading';

const styles = (): StyleRules => {
  return {
    root: {
      padding: '30px 40px',
    },
    numInstances: {
      width: '200px',
    },
  };
};

const OffsetBasePathEditor = ({ onChange, value }) => {
  const widget = {
    label: 'Offset base path',
    name: 'offset',
    'widget-type': 'textbox',
    'widget-attributes': {
      placeholder: 'Path for offset storage location',
    },
  };

  const property = {
    required: false,
    name: 'offset',
  };

  return (
    <WidgetWrapper
      widgetProperty={widget}
      pluginProperty={property}
      value={value}
      onChange={onChange}
    />
  );
};

const NumInstancesEditor = ({ onChange, value }) => {
  const widget = {
    label: 'Number of instances',
    name: 'numInstance',
    'widget-type': 'number',
    'widget-attributes': {
      min: 1,
    },
  };

  const property = {
    required: true,
    name: 'numInstance',
  };

  function handleChange(val) {
    onChange(parseInt(val, 10));
  }

  return (
    <WidgetWrapper
      widgetProperty={widget}
      pluginProperty={property}
      value={value.toString()}
      onChange={handleChange}
    />
  );
};

const AdvancedView: React.FC<ICreateContext & WithStyles<typeof styles>> = ({
  classes,
  offsetBasePath,
  numInstances,
  setAdvanced,
}) => {
  const [localOffset, setLocalOffset] = React.useState(offsetBasePath);
  const [localNumInstances, setLocalNumInstances] = React.useState(numInstances);

  function handleNext() {
    setAdvanced(localOffset, localNumInstances);
  }

  return (
    <div className={classes.root}>
      <Heading type={HeadingTypes.h3} label="Advanced" />
      <br />

      <OffsetBasePathEditor value={localOffset} onChange={setLocalOffset} />

      <br />
      <div className={classes.numInstances}>
        <NumInstancesEditor value={localNumInstances} onChange={setLocalNumInstances} />
      </div>

      <StepButtons onNext={handleNext} />
    </div>
  );
};

const StyledAdvanced = withStyles(styles)(AdvancedView);
const Advanced = createContextConnect(StyledAdvanced);
export default Advanced;

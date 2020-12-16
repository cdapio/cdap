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
import FormControlLabel from '@material-ui/core/FormControlLabel';
import Radio from '@material-ui/core/Radio';
import RadioGroup from '@material-ui/core/RadioGroup';
import If from 'components/If';

const styles = (): StyleRules => {
  return {
    root: {
      padding: '30px 40px',
    },
    taskSection: {
      marginBottom: '35px',
    },
    numInstances: {
      width: '200px',
      marginLeft: '25px',
    },
    dataAmountContainer: {
      padding: '0 25px',
    },
    dataAmountSelector: {
      marginTop: '15px',
      width: '200px',
    },
    radioGroup: {
      flexDirection: 'initial',
      marginBottom: '5px',
      '& > label': {
        marginRight: '40px',
      },
    },
  };
};

const OffsetBasePathEditor = ({ onChange, value }) => {
  const widget = {
    label: 'Path',
    name: 'offset',
    'widget-type': 'textbox',
    'widget-attributes': {
      placeholder: 'Path for checkpoint storage location',
    },
  };

  const property = {
    required: false,
    name: 'offset',
    description:
      'This is the directory where checkpoints for the replication pipeline are stored, so the pipeline can resume from a previous checkpoint, instead of starting from the beginning if it is restarted.',
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
    label: 'Number of tasks',
    name: 'numInstance',
    'widget-type': 'number',
    'widget-attributes': {
      min: 1,
    },
  };

  const property = {
    required: true,
    name: 'numInstance',
    description:
      'The tables in a replication pipeline are evenly distributed amongst all the tasks. Set this to a higher number to distribute the load amongst a larger number of tasks, thereby increasing the parallelism of the replication pipeline. This value cannot be changed after the pipeline is created.',
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

const options = [
  {
    value: 1,
    label: '< 1 GB / hr',
  },
];

for (let i = 1; i < 10; i++) {
  const SUFFIX = 'GB / hr';
  const MULTIPLIER = 10;
  const VALUE_MULTIPLIER = 2;
  const from = i * MULTIPLIER + 1;
  const to = (i + 1) * MULTIPLIER;

  options.push({
    value: i * VALUE_MULTIPLIER,
    label: `${from} - ${to} ${SUFFIX}`,
  });
}

const DataAmountSelector = ({ onChange, value }) => {
  const widget = {
    label: 'Number of tasks',
    name: 'numInstance',
    'widget-type': 'select',
    'widget-attributes': {
      options,
    },
  };

  const property = {
    required: false,
    name: 'dataAmount',
  };

  return (
    <WidgetWrapper
      widgetProperty={widget}
      pluginProperty={property}
      value={value.toString()}
      onChange={onChange}
      hideLabel={true}
    />
  );
};

const TASK_OPTIONS = {
  manual: 'Set number of tasks manually',
  calculate: 'Calculate number of tasks based on data',
};

const AdvancedView: React.FC<ICreateContext & WithStyles<typeof styles>> = ({
  classes,
  offsetBasePath,
  numInstances,
  setAdvanced,
}) => {
  const [localOffset, setLocalOffset] = React.useState(offsetBasePath);
  const [localNumInstances, setLocalNumInstances] = React.useState(numInstances);
  const [taskSelection, setTaskSelection] = React.useState(TASK_OPTIONS.calculate);
  const [dataAmount, setDataAmount] = React.useState(1);

  React.useEffect(() => {
    const initialSelection = getInitialTaskSelection();
    setTaskSelection(initialSelection);
  }, []);

  function getInitialTaskSelection() {
    const initialDataAmount = options.find((opt) => opt.value === numInstances);
    if (initialDataAmount) {
      setDataAmount(initialDataAmount.value);
      return TASK_OPTIONS.calculate;
    }

    return TASK_OPTIONS.manual;
  }

  function handleNext() {
    let selectedNumInstance = localNumInstances;
    if (taskSelection === TASK_OPTIONS.calculate) {
      selectedNumInstance = dataAmount;
    }

    setAdvanced(localOffset, selectedNumInstance);
  }

  function handleSelectTask(e) {
    setTaskSelection(e.target.value);
  }

  return (
    <div className={classes.root}>
      <Heading type={HeadingTypes.h3} label="Configure advanced properties" />
      <br />

      <div className={classes.taskSection}>
        <Heading type={HeadingTypes.h4} label="Tasks" />
        <div className={classes.numTasksRadio}>
          <RadioGroup
            value={taskSelection}
            onChange={handleSelectTask}
            className={classes.radioGroup}
          >
            <FormControlLabel
              value={TASK_OPTIONS.calculate}
              control={<Radio color="primary" />}
              label={TASK_OPTIONS.calculate}
            />
            <FormControlLabel
              value={TASK_OPTIONS.manual}
              control={<Radio color="primary" />}
              label={TASK_OPTIONS.manual}
            />
          </RadioGroup>
        </div>
        <If condition={taskSelection === TASK_OPTIONS.manual}>
          <div className={classes.numInstances}>
            <NumInstancesEditor value={localNumInstances} onChange={setLocalNumInstances} />
          </div>
        </If>
        <If condition={taskSelection === TASK_OPTIONS.calculate}>
          <div className={classes.dataAmountContainer}>
            <div>How much data are you replicating in an hour?</div>
            <div className={classes.dataAmountSelector}>
              <DataAmountSelector value={dataAmount} onChange={setDataAmount} />
            </div>
          </div>
        </If>
      </div>

      <Heading type={HeadingTypes.h4} label="Checkpoint" />
      <br />
      <OffsetBasePathEditor value={localOffset} onChange={setLocalOffset} />

      <StepButtons onNext={handleNext} />
    </div>
  );
};

const StyledAdvanced = withStyles(styles)(AdvancedView);
const Advanced = createContextConnect(StyledAdvanced);
export default Advanced;

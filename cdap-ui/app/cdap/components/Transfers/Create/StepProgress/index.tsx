/*
 * Copyright © 2019 Cask Data, Inc.
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
import { transfersCreateConnect } from 'components/Transfers/Create/context';
import { StageConfiguration } from 'components/Transfers/Create/Content';
import { objectQuery } from 'services/helpers';
import classnames from 'classnames';

const styles = (theme): StyleRules => {
  return {
    root: {
      backgroundColor: theme.palette.white[50],
    },
    stepRow: {
      padding: '15px 50px',
      borderBottom: `1px solid ${theme.palette.grey[300]}`,
      cursor: 'pointer',
      '&:hover': {
        backgroundColor: theme.palette.grey[600],
      },
    },
    disabled: {
      color: theme.palette.grey[400],
      cursor: 'not-allowed',
    },
    active: {
      backgroundColor: theme.palette.grey[600],
      fontStyle: 'italic',
      paddingLeft: '55px',
    },
  };
};

interface IStepProgressProps extends WithStyles<typeof styles> {
  activeStep: number;
  stage: string;
  setActiveStep: (step) => void;
}

const StepProgressView: React.SFC<IStepProgressProps> = ({
  activeStep,
  stage,
  setActiveStep,
  classes,
}) => {
  const steps = objectQuery(StageConfiguration, stage, 'steps');

  function handleStepClick(step) {
    if (step >= activeStep) {
      return;
    }

    setActiveStep(step);
  }

  return (
    <div className={classes.root}>
      {steps.map((step, i) => {
        return (
          <div
            className={classnames(classes.stepRow, {
              [classes.disabled]: i > activeStep,
              [classes.active]: i === activeStep,
            })}
            key={step.label}
            onClick={handleStepClick.bind(null, i)}
          >
            {step.label}
          </div>
        );
      })}
    </div>
  );
};

const StyledStepProgress = withStyles(styles)(StepProgressView);
const StepProgress = transfersCreateConnect(StyledStepProgress);
export default StepProgress;

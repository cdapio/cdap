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
import { createContextConnect } from 'components/PluginCreator/Create';
import Chip from '@material-ui/core/Chip';
import { STEPS } from 'components/PluginCreator/Create/steps';
import classnames from 'classnames';
import Check from '@material-ui/icons/Check';

const styles = (theme): StyleRules => {
  return {
    root: {
      borderRight: `1px solid ${theme.palette.grey[300]}`,
    },
    row: {
      padding: '15px',
      lineHeight: '24px',
    },
    finishedRow: {
      cursor: 'pointer',

      '&:hover': {
        backgroundColor: theme.palette.grey[700],
      },

      '& $chip': {
        backgroundColor: theme.palette.blue[100],
      },
    },
    active: {
      backgroundColor: theme.palette.grey[700],
    },
    chip: {
      marginRight: '10px',
      color: theme.palette.white[50],
      backgroundColor: theme.palette.grey[200],
      minWidth: '24px', // weird thing with material-ui Chip component
      height: '24px',

      '& span': {
        paddingLeft: '5px',
        paddingRight: '5px',
      },
    },
    checkIcon: {
      fontSize: '14px',
    },
  };
};

interface ILeftPanelProps extends WithStyles<typeof styles> {
  activeStep: number;
  setActiveStep: (step: number) => void;
}

const LeftPanelView: React.FC<ILeftPanelProps> = ({ classes, setActiveStep, activeStep }) => {
  function handleStepClick(step) {
    if (step >= activeStep) {
      return;
    }

    setActiveStep(step);
  }

  return (
    <div className={classes.root}>
      {STEPS.map((step, i) => {
        const chipContent = i < activeStep ? <Check className={classes.checkIcon} /> : i + 1;

        return (
          <div
            key={step.label}
            className={classnames(classes.row, {
              [classes.finishedRow]: i <= activeStep,
              [classes.active]: i === activeStep,
            })}
            onClick={handleStepClick.bind(null, i)}
          >
            <Chip label={chipContent} className={classes.chip} />
            <span>{step.label}</span>
          </div>
        );
      })}
    </div>
  );
};

const StyledLeftPanel = withStyles(styles)(LeftPanelView);
const LeftPanel = createContextConnect(StyledLeftPanel);
export default LeftPanel;

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

import Chip from '@material-ui/core/Chip';
import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';
import Check from '@material-ui/icons/Check';
import classnames from 'classnames';
import { useAppInternalState } from 'components/PluginJSONCreator/Create';
import { STEPS } from 'components/PluginJSONCreator/Create/Content';
import * as React from 'react';

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
      minWidth: '24px',
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

const StepsGuidelineMenuView: React.FC<WithStyles<typeof styles>> = ({ classes }) => {
  const { activeStep, setActiveStep } = useAppInternalState();

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

const StepsGuidelineMenu = withStyles(styles)(StepsGuidelineMenuView);
export default StepsGuidelineMenu;

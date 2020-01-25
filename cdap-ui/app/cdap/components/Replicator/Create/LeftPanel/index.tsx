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
import { createContextConnect } from 'components/Replicator/Create';
import Chip from '@material-ui/core/Chip';
import { STEPS } from 'components/Replicator/Create/steps';
import classnames from 'classnames';

const styles = (theme): StyleRules => {
  return {
    root: {
      borderRight: `1px solid ${theme.palette.grey[300]}`,
    },
    row: {
      padding: '15px',
      cursor: 'pointer',
      lineHeight: '24px',
      '&:hover': {
        backgroundColor: theme.palette.grey[700],
      },
    },
    active: {
      backgroundColor: theme.palette.grey[700],
      '& $chip': {
        backgroundColor: theme.palette.blue[100],
      },
    },
    chip: {
      marginRight: '10px',
      color: theme.palette.white[50],
      backgroundColor: theme.palette.grey[200],
      minWidth: '24px', // weird thing with material-ui Chip component
    },
  };
};

interface ILeftPanelProps extends WithStyles<typeof styles> {
  activeStep: number;
  setActiveStep: (step: number) => void;
}

const LeftPanelView: React.FC<ILeftPanelProps> = ({ classes, setActiveStep, activeStep }) => {
  return (
    <div className={classes.root}>
      {STEPS.map((step, i) => {
        return (
          <div
            key={step.label}
            className={classnames(classes.row, { [classes.active]: activeStep === i })}
            onClick={setActiveStep.bind(null, i)}
          >
            <Chip size="small" label={i + 1} className={classes.chip} />
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

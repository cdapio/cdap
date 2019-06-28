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
import { transfersCreateConnect, Stages } from 'components/Transfers/Create/context';
import ExpansionPanel from '@material-ui/core/ExpansionPanel';
import ExpansionPanelSummary from '@material-ui/core/ExpansionPanelSummary';
import ExpansionPanelDetails from '@material-ui/core/ExpansionPanelDetails';
import StepProgress from '../StepProgress';
import { StageConfiguration } from 'components/Transfers/Create/Content';

const styles = (theme): StyleRules => {
  return {
    root: {
      width: '25vw',
      backgroundColor: theme.palette.grey[600],
      borderRight: `1px solid ${theme.palette.grey[400]}`,
      borderTop: `1px solid ${theme.palette.grey[400]}`,
    },
  };
};

interface IProps extends WithStyles<typeof styles> {
  stage: string;
}

const LeftPanelView: React.SFC<IProps> = ({ classes, stage }) => {
  return (
    <div className={classes.root}>
      <ExpansionPanel square expanded={stage === Stages.CONFIGURE}>
        <ExpansionPanelSummary>
          <div>{StageConfiguration[Stages.CONFIGURE].label}</div>
        </ExpansionPanelSummary>
        <ExpansionPanelDetails>
          <StepProgress />
        </ExpansionPanelDetails>
      </ExpansionPanel>
      <ExpansionPanel
        square
        expanded={stage === Stages.ASSESSMENT}
        disabled={stage === Stages.CONFIGURE}
      >
        <ExpansionPanelSummary>
          <div>{StageConfiguration[Stages.ASSESSMENT].label}</div>
        </ExpansionPanelSummary>
        <ExpansionPanelDetails>
          <StepProgress />
        </ExpansionPanelDetails>
      </ExpansionPanel>
      <ExpansionPanel
        square
        expanded={stage === Stages.PUBLISH}
        disabled={stage !== Stages.PUBLISH}
      >
        <ExpansionPanelSummary>
          <div>{StageConfiguration[Stages.PUBLISH].label}</div>
        </ExpansionPanelSummary>
        <ExpansionPanelDetails>
          <StepProgress />
        </ExpansionPanelDetails>
      </ExpansionPanel>
    </div>
  );
};

const StyledLeftPanel = withStyles(styles)(LeftPanelView);
const LeftPanel = transfersCreateConnect(StyledLeftPanel);
export default LeftPanel;

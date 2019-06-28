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
import Summary from '../../Summary';
import StepButtons from '../../StepButtons';

const styles = (): StyleRules => {
  return {};
};

interface IConfigureSummary extends WithStyles<typeof styles> {
  setStage: (stage) => void;
}

const ConfigureSummaryView: React.SFC<IConfigureSummary> = ({ setStage }) => {
  function onComplete() {
    setStage(Stages.ASSESSMENT);
  }

  return (
    <div>
      <Summary />
      <br />
      <StepButtons onComplete={onComplete} />
    </div>
  );
};

const StyledConfigureSummary = withStyles(styles)(ConfigureSummaryView);
const ConfigureSummary = transfersCreateConnect(StyledConfigureSummary);
export default ConfigureSummary;

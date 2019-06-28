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
import { transfersCreateConnect } from 'components/Transfers/Create/context';
import Stepper from '@material-ui/core/Stepper';
import Step from '@material-ui/core/Step';
import StepLabel from '@material-ui/core/StepLabel';
import { StageConfiguration } from 'components/Transfers/Create/Content';
import { objectQuery } from 'services/helpers';

interface IStepProgressProps {
  activeStep: number;
  stage: string;
}

const StepProgressView: React.SFC<IStepProgressProps> = ({ activeStep, stage }) => {
  const steps = objectQuery(StageConfiguration, stage, 'steps');

  return (
    <Stepper activeStep={activeStep} orientation="vertical">
      {steps.map((step) => {
        return (
          <Step key={step.label}>
            <StepLabel>{step.label}</StepLabel>
          </Step>
        );
      })}
    </Stepper>
  );
};

const StepProgress = transfersCreateConnect(StepProgressView);
export default StepProgress;

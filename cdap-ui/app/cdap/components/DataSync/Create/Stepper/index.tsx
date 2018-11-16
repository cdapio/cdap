/*
 * Copyright © 2018 Cask Data, Inc.
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
import { connect } from 'react-redux';
import Stepper from '@material-ui/core/Stepper';
import Step from '@material-ui/core/Step';
import StepLabel from '@material-ui/core/StepLabel';

interface IStepperProps {
  activeStep: number;
}

const STEPS = [
  'Select Source',
  'Configure Source',
  'Select Sink',
  'Configure Sink',
  'Advanced Configuration',
  'Review',
];

const StepsView: React.SFC<IStepperProps> = ({ activeStep }) => {
  return (
    <Stepper activeStep={activeStep}>
      {STEPS.map((label) => {
        return (
          <Step key={label}>
            <StepLabel>{label}</StepLabel>
          </Step>
        );
      })}
    </Stepper>
  );
};

const mapStateToProps = (state) => {
  return {
    activeStep: state.datasync.activeStep,
  };
};

const Steps = connect(mapStateToProps)(StepsView);

export default Steps;

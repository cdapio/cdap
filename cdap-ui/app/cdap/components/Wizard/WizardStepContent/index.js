/*
 * Copyright © 2016 Cask Data, Inc.
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

import React, {PropTypes} from 'react';
require('./WizardStepContent.less');

export default function WizardStepContent(props) {
  const {
    title,
    description,
    stepsCount,
    currentStep,
    children
  } = props;
  return (
    <div className="wizard-step-content-container">
      <div className="step-banner clearfix">
        <div className="pull-left">
          <h4>{title}</h4>
          <p>{description}</p>
        </div>
        <div className="pull-right progress-counter">
          {currentStep} / {stepsCount}
        </div>
      </div>
      <div className="step-content">
        {children}
      </div>
    </div>
  );
}

WizardStepContent.propTypes = {
  title: PropTypes.string,
  description: PropTypes.string,
  stepsCount: PropTypes.number,
  currentStep: PropTypes.number,
  children: PropTypes.node
};

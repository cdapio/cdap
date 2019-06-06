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
import IconButton from '@material-ui/core/IconButton';
import ChevronLeft from '@material-ui/icons/ChevronLeft';
import ChevronRight from '@material-ui/icons/ChevronRight';
import './PaginationStepper.scss';

interface IPaginationStepperProps {
  onPrev: () => void;
  onNext: () => void;
  prevDisabled: boolean;
  nextDisabled: boolean;
}

const PaginationStepper: React.SFC<IPaginationStepperProps> = ({
  onPrev,
  onNext,
  prevDisabled,
  nextDisabled,
}) => {
  return (
    <div className="pagination-stepper-container">
      <IconButton onClick={onPrev} disabled={prevDisabled} className="step-button">
        <ChevronLeft />
      </IconButton>

      <IconButton onClick={onNext} disabled={nextDisabled} className="step-button">
        <ChevronRight />
      </IconButton>
    </div>
  );
};

export default PaginationStepper;

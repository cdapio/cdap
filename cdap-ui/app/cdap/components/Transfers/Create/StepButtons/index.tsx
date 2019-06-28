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
import Button from '@material-ui/core/Button';
import If from 'components/If';

const styles = (): StyleRules => {
  return {
    root: {
      '& > button': {
        marginRight: '25px',
      },
    },
  };
};

interface IStepButtonsProps extends WithStyles<typeof styles> {
  activeStep: number;
  onNext?: () => void;
  next: () => void;
  previous: () => void;
  onComplete?: () => void;
}

const StepButtonsView: React.SFC<IStepButtonsProps> = ({
  activeStep,
  onNext,
  next,
  previous,
  onComplete,
  classes,
}) => {
  function handleNextClick() {
    onNext();
    next();
  }

  return (
    <div className={classes.root}>
      <If condition={activeStep > 0}>
        <Button color="primary" onClick={previous}>
          Previous
        </Button>
      </If>
      <If condition={typeof onComplete !== 'function'}>
        <Button variant="contained" color="primary" onClick={handleNextClick}>
          Next
        </Button>
      </If>
      <If condition={typeof onComplete === 'function'}>
        <Button variant="contained" color="primary" onClick={onComplete}>
          Create transfer
        </Button>
      </If>
    </div>
  );
};

const StyledStepButtons = withStyles(styles)(StepButtonsView);
const StepButtons = transfersCreateConnect(StyledStepButtons);
export default StepButtons;

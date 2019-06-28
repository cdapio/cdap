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
import ArrowRight from '@material-ui/icons/ArrowRightAlt';
import Assessment from '@material-ui/icons/Assessment';
import Button from '@material-ui/core/Button';
import { transfersCreateConnect } from '../../context';

const styles = (): StyleRules => {
  return {
    arrow: {
      fontSize: '28px',
      margin: '0 15px',
    },
    buttonContainer: {
      marginTop: '25px',
    },
    button: {
      fontSize: '1.5rem',
    },
    assessmentIcon: {
      marginRight: '10px',
      fontSize: '1.8rem',
    },
  };
};

interface IProps extends WithStyles<typeof styles> {
  next: () => void;
}

const GenerateAssessmentView: React.SFC<IProps> = ({ classes, next }) => {
  return (
    <div>
      <div>
        <h2 className="text-center">
          <span>MySQL</span>
          <ArrowRight className={classes.arrow} />
          <span>Google BigQuery</span>
        </h2>
      </div>

      <div className={`text-center ${classes.buttonContainer}`}>
        <Button
          className={classes.button}
          variant="contained"
          size="large"
          color="primary"
          onClick={next}
        >
          <Assessment className={classes.assessmentIcon} />
          Generate Assessment
        </Button>
      </div>
    </div>
  );
};

const StyledGenerateAssessment = withStyles(styles)(GenerateAssessmentView);
const GenerateAssessment = transfersCreateConnect(StyledGenerateAssessment);
export default GenerateAssessment;

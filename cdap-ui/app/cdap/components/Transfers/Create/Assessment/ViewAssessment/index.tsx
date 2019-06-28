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
import { transfersCreateConnect, Stages } from '../../context';
import StepButtons from '../../StepButtons';
import CheckCircle from '@material-ui/icons/CheckCircle';
import SchemaAssessment from '../SchemaAssessment';
import If from 'components/If';
import LoadingSVG from 'components/LoadingSVG';
import TABLES from '../SchemaAssessment/tablesDefinition';
import moment from 'moment';

const styles = (theme): StyleRules => {
  return {
    section: {
      borderBottom: `2px solid ${theme.palette.grey[400]}`,
      padding: '25px',
    },
    halfSection: {
      display: 'grid',
      gridTemplateColumns: '50% 50%',
      fontSize: '1.5rem',
    },
    successIcon: {
      color: theme.palette.green[100],
      fontSize: '18px',
      marginRight: '10px',
    },
  };
};

interface IProps extends WithStyles<typeof styles> {
  sourceConfig: any;
  setStage: (stage: string) => void;
}

const ViewAssessmentView: React.SFC<IProps> = ({ classes, sourceConfig, setStage }) => {
  const [showStage, setShowStage] = React.useState(0);

  React.useEffect(() => {
    setTimeout(() => {
      setShowStage(1);
    }, 1000);
    setTimeout(() => {
      setShowStage(2);
    }, 3000);
    setTimeout(() => {
      setShowStage(3);
    }, 6000);
    setTimeout(() => {
      setShowStage(4);
    }, 8000);
  }, []);

  return (
    <div>
      <h1>Assessment Report</h1>
      <small>{moment().format('MMM D, YYYY [at] hh:mm A')}</small>
      <hr />
      <If condition={showStage > 0}>
        <div className={classes.section}>
          <h2>Network Assessment</h2>
          <br />
          <div className={classes.halfSection}>
            <div>
              <CheckCircle className={classes.successIcon} />
              MySQL
              <h3 />
            </div>
            <div>
              <CheckCircle className={classes.successIcon} />
              Google BigQuery
            </div>
          </div>
        </div>
      </If>

      <If condition={showStage > 1}>
        <div className={classes.section}>
          <h2>Permission Assessment</h2>
          <br />
          <div className={classes.halfSection}>
            <div>
              <div>
                <strong>MySQL</strong>
              </div>

              <div>
                <strong>User:</strong> replicate
              </div>
              <br />
              <div>
                <CheckCircle className={classes.successIcon} />
                User has the required permission.
              </div>
            </div>

            <div>
              <div>
                <strong>Google BigQuery</strong>
              </div>
              <br />
              <div>
                <div>
                  <CheckCircle className={classes.successIcon} />
                  Service account has the required role.
                </div>
              </div>
            </div>
          </div>
        </div>
      </If>

      <If condition={showStage > 2}>
        <div className={classes.section}>
          <h2>Statistics</h2>
          <div>
            <strong># tables: </strong>
            <span>{TABLES.length}</span>
          </div>
          <div>
            <strong># rows: </strong>
            <span>5,123,412,649,083</span>
          </div>
        </div>
      </If>

      <If condition={showStage > 3}>
        <div className={classes.section}>
          <h2>Schema Assessment</h2>
          <SchemaAssessment />
        </div>
      </If>

      <If condition={showStage < 4}>
        <div className="text-center">
          <br />
          <LoadingSVG />
          <br />
        </div>
      </If>

      <br />
      <StepButtons onComplete={setStage.bind(null, Stages.PUBLISH)} />
    </div>
  );
};

const StyledViewAssessment = withStyles(styles)(ViewAssessmentView);
const ViewAssessment = transfersCreateConnect(StyledViewAssessment);
export default ViewAssessment;

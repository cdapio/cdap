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
import { transfersCreateConnect } from '../../context';
import StepButtons from '../../StepButtons';
import classnames from 'classnames';
import If from 'components/If';
import Button from '@material-ui/core/Button';
import AssessmentTable from './AssessmentTable';

const styles = (theme): StyleRules => {
  return {
    tab: {
      display: 'flex',
      marginTop: '15px',
      '& > div': {
        width: '200px',
        fontSize: '16px',
        cursor: 'pointer',
      },
    },
    activeTab: {
      fontWeight: 'bold',
      '& > span': {
        borderBottom: `3px solid ${theme.palette.grey[300]}`,
      },
    },
  };
};

const schemaIssues = [
  {
    name: 'Table 1',
    numColumns: 56,
    schemaIssues: 4,
    partialSupport: 2,
    notSupported: 2,
  },
  {
    name: 'Table 2',
    numColumns: 14,
    schemaIssues: 4,
    partialSupport: 0,
    notSupported: 2,
  },
  {
    name: 'Table 3',
    numColumns: 145,
    schemaIssues: 4,
    partialSupport: 2,
    notSupported: 1,
  },
  {
    name: 'Table 4',
    numColumns: 23,
    schemaIssues: 2,
    partialSupport: 2,
    notSupported: 0,
  },
  {
    name: 'Table 5',
    numColumns: 44,
    schemaIssues: 2,
    partialSupport: 2,
    notSupported: 0,
  },
  {
    name: 'Table 6',
    numColumns: 37,
    schemaIssues: 1,
    partialSupport: 1,
    notSupported: 0,
  },
  {
    name: 'Table 7',
    numColumns: 12,
    schemaIssues: 1,
    partialSupport: 0,
    notSupported: 1,
  },
];

const goodTables = [
  {
    name: 'Table 8',
    numColumns: 34,
    schemaIssues: 0,
    partialSupport: 0,
    notSupported: 0,
  },
  {
    name: 'Table 9',
    numColumns: 124,
    schemaIssues: 0,
    partialSupport: 0,
    notSupported: 0,
  },
  {
    name: 'Table 10',
    numColumns: 25,
    schemaIssues: 0,
    partialSupport: 0,
    notSupported: 0,
  },
];

interface IProps extends WithStyles<typeof styles> {
  sourceConfig: any;
}

const ViewAssessmentView: React.SFC<IProps> = ({ classes }) => {
  const [activeTab, setActiveTab] = React.useState(0);

  function handleTabSwitch(tab) {
    setActiveTab(tab);
  }

  return (
    <div>
      <h2>Assessment summary</h2>
      <div>Resolve all issues to continue</div>

      <div className={classes.tab}>
        <div
          onClick={handleTabSwitch.bind(null, 0)}
          className={classnames({ [classes.activeTab]: activeTab === 0 })}
        >
          <span>Schema issues (7)</span>
        </div>
        <div
          onClick={handleTabSwitch.bind(null, 1)}
          className={classnames({ [classes.activeTab]: activeTab === 1 })}
        >
          <span>Missing features (1)</span>
        </div>
        <div
          onClick={handleTabSwitch.bind(null, 2)}
          className={classnames({ [classes.activeTab]: activeTab === 2 })}
        >
          <span>Connectivity issues (1)</span>
        </div>
      </div>

      <If condition={activeTab === 0}>
        <div>
          <div>7 tables have been assessed with schema issues</div>
          <AssessmentTable tables={schemaIssues} />

          <div className="text-right">
            <Button variant="outlined" color="primary">
              Auto resolve
            </Button>
          </div>

          <br />

          <div>
            <div>3 tables ave been assessed with no schema issues</div>
            <AssessmentTable tables={goodTables} />
          </div>
        </div>
      </If>

      <StepButtons />
    </div>
  );
};

const StyledViewAssessment = withStyles(styles)(ViewAssessmentView);
const ViewAssessment = transfersCreateConnect(StyledViewAssessment);
export default ViewAssessment;

/*
 * Copyright © 2020 Cask Data, Inc.
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
import If from 'components/If';
import Mappings from './Mappings';
import WithIssuesTables from 'components/Replicator/Create/Content/Assessment/TablesAssessment/WithIssuesTables';
import NoIssuesTables from 'components/Replicator/Create/Content/Assessment/TablesAssessment/NoIssuesTables';

const styles = (): StyleRules => {
  return {
    tableContainer: {
      marginBottom: '50px',
    },
  };
};

export interface ITable {
  database: string;
  table: string;
  numColumns: number;
  numColumnsPartiallySupported: number;
  numColumnsNotSupported: number;
}

interface ITablesAssessmentProps extends WithStyles<typeof styles> {
  tables: ITable[];
}

const TablesAssessmentView: React.FC<ITablesAssessmentProps> = ({ classes, tables }) => {
  const [openTable, setOpenTable] = React.useState(null);
  const [tablesWithIssues, setTablesWithIssues] = React.useState([]);
  const [tablesNoIssues, setTablesNoIssues] = React.useState([]);

  React.useEffect(() => {
    const updatedTablesWithIssues = [];
    const updatedTablesNoIssues = [];

    tables.forEach((table) => {
      if (table.numColumnsPartiallySupported === 0 && table.numColumnsNotSupported === 0) {
        updatedTablesNoIssues.push(table);
      } else {
        updatedTablesWithIssues.push(table);
      }
    });

    setTablesWithIssues(updatedTablesWithIssues);
    setTablesNoIssues(updatedTablesNoIssues);
  }, [tables]);

  return (
    <React.Fragment>
      <div className={classes.tableContainer}>
        <WithIssuesTables tables={tablesWithIssues} setOpenTable={setOpenTable} />
      </div>

      <div className={classes.tableContainer}>
        <NoIssuesTables tables={tablesNoIssues} setOpenTable={setOpenTable} />
      </div>

      <If condition={openTable}>
        <Mappings tableInfo={openTable} onClose={() => setOpenTable(null)} />
      </If>
    </React.Fragment>
  );
};

const TablesAssessment = withStyles(styles)(TablesAssessmentView);
export default TablesAssessment;

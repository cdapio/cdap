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
import { createContextConnect, ICreateContext } from 'components/Replicator/Create';
import { generateTableKey } from 'components/Replicator/utilities';

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

interface ITablesAssessmentProps extends WithStyles<typeof styles>, ICreateContext {
  assessmentTables: ITable[];
  runAssessment: () => void;
}

const TablesAssessmentView: React.FC<ITablesAssessmentProps> = ({
  classes,
  tables,
  assessmentTables,
  runAssessment,
}) => {
  const [openTable, setOpenTable] = React.useState(null);
  const [tablesWithIssues, setTablesWithIssues] = React.useState([]);
  const [tablesNoIssues, setTablesNoIssues] = React.useState([]);
  const [haveMissingTables, setHaveMissingTables] = React.useState(0);

  React.useEffect(() => {
    const updatedTablesWithIssues = [];
    const updatedTablesNoIssues = [];

    let nonAssessedTables = tables;
    assessmentTables.forEach((table) => {
      const tableKey = generateTableKey(table);
      if (nonAssessedTables.get(tableKey)) {
        nonAssessedTables = nonAssessedTables.remove(tableKey);
      }

      if (table.numColumnsPartiallySupported === 0 && table.numColumnsNotSupported === 0) {
        updatedTablesNoIssues.push(table);
      } else {
        updatedTablesWithIssues.push(table);
      }
    });

    if (nonAssessedTables.size > 0) {
      setHaveMissingTables(nonAssessedTables.size);
    }

    setTablesWithIssues(updatedTablesWithIssues);
    setTablesNoIssues(updatedTablesNoIssues);
  }, [assessmentTables]);

  function onMappingClose(rerunAssessment) {
    setOpenTable(null);
    if (rerunAssessment) {
      runAssessment();
    }
  }

  return (
    <React.Fragment>
      <If condition={haveMissingTables > 0}>
        <div>
          There{' '}
          {haveMissingTables === 1
            ? `is ${haveMissingTables} table`
            : `are ${haveMissingTables} tables`}{' '}
          that cannot be assessed. Please check the Connectivity issues tab.
        </div>
      </If>

      <If condition={!(haveMissingTables > 0 && tablesWithIssues.length === 0)}>
        <div className={classes.tableContainer}>
          <WithIssuesTables tables={tablesWithIssues} setOpenTable={setOpenTable} />
        </div>
      </If>

      <div className={classes.tableContainer}>
        <NoIssuesTables tables={tablesNoIssues} setOpenTable={setOpenTable} />
      </div>

      <If condition={openTable}>
        <Mappings tableInfo={openTable} onClose={onMappingClose} />
      </If>
    </React.Fragment>
  );
};

const StyledTablesAssessment = withStyles(styles)(TablesAssessmentView);
const TablesAssessment = createContextConnect(StyledTablesAssessment);
export default TablesAssessment;

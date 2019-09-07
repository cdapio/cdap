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
import StepButtons from '../../StepButtons';
import { getCurrentNamespace } from 'services/NamespaceStore';
import { MyDeltaApi } from 'api/delta';
import If from 'components/If';
import { Checkbox } from '@material-ui/core';

const styles = (theme): StyleRules => {
  return {
    contentContainer: {
      display: 'grid',
      gridTemplateColumns: '1fr 2fr',
      gridColumnGap: '50px',
      height: '60vh',
      '& > div': {
        height: '100%',
        overflowY: 'auto',
      },
    },
    preview: {
      color: theme.palette.blue[100],
      cursor: 'pointer',
    },
    tableContainer: {
      '& .table th': {
        verticalAlign: 'middle',
      },
      '& .table td': {
        verticalAlign: 'middle',
        '&:first-child': {
          width: '50px',
        },
        '&:last-child': {
          width: '75px',
        },
      },
    },
  };
};

interface ISelectTable extends WithStyles<typeof styles> {
  source: {
    host: string;
    port: string;
    user: string;
    password: string;
  };
}

const SelectTableView: React.SFC<ISelectTable> = ({ source, classes }) => {
  const [tables, setTables] = React.useState([]);
  const [error, setError] = React.useState(null);
  const [data, setData] = React.useState(null);
  const [activeTable, setActiveTable] = React.useState(null);
  const [selected, setSelected] = React.useState([]);

  const requestBody = {
    host: source.host,
    port: source.port,
    user: source.user,
    password: source.password,
  };

  React.useEffect(() => {
    const params = {
      context: getCurrentNamespace(),
      database: 'demo',
    };

    MyDeltaApi.getTables(params, requestBody).subscribe(setTables, setError);
  }, []);

  function fetchData(table) {
    const params = {
      context: getCurrentNamespace(),
      database: 'demo',
      table,
    };

    MyDeltaApi.sampleData(params, requestBody).subscribe(setData, setError);
  }

  function handlePreviewClick(table) {
    if (activeTable === table) {
      setActiveTable(null);
      setData(null);
    } else {
      setActiveTable(table);
      fetchData(table);
    }
  }

  function toggleTable(table) {
    const newSelected = [...selected];
    const index = newSelected.indexOf(table);

    if (index === -1) {
      newSelected.push(table);
    } else {
      newSelected.splice(index, 1);
    }

    setSelected(newSelected);
  }

  function renderData() {
    if (!data) {
      return null;
    }

    return (
      <div>
        <h2>{activeTable} - preview</h2>

        <div>
          <table className="table">
            <thead>
              <tr>
                {data.columns.map((header) => {
                  return (
                    <th key={header.name}>
                      {header.name} ({header.type})
                    </th>
                  );
                })}
              </tr>
            </thead>

            <tbody>
              {data.data.map((row, i) => {
                return (
                  <tr key={i}>
                    {data.columns.map((column, j) => {
                      return <td key={j}>{row[column.name]}</td>;
                    })}
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>
    );
  }

  function toggleAll() {
    if (selected.length === tables.length) {
      setSelected([]);
    } else {
      setSelected([...tables]);
    }
  }

  return (
    <div>
      <If condition={error}>
        <div className="text-danger">{JSON.stringify(error, null, 2)}</div>
      </If>

      <div className={classes.contentContainer}>
        <div className={classes.tableContainer}>
          <h2>Select tables to replicate</h2>
          <div>
            {selected.length} of {tables.length} tables selected
          </div>
          <table className="table">
            <thead>
              <tr>
                <th>
                  <Checkbox
                    checked={selected.length === tables.length}
                    onChange={toggleAll}
                    color="primary"
                  />
                </th>
                <th>Table</th>
                <th />
              </tr>
            </thead>

            <tbody>
              {tables.map((table) => {
                return (
                  <tr key={table}>
                    <td>
                      <Checkbox
                        checked={selected.indexOf(table) !== -1}
                        onChange={toggleTable.bind(null, table)}
                        color="primary"
                      />
                    </td>
                    <td>{table}</td>
                    <td>
                      <span
                        className={classes.preview}
                        onClick={handlePreviewClick.bind(null, table)}
                      >
                        {activeTable === table ? 'Hide' : 'Preview'}
                      </span>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>

        {renderData()}
      </div>

      <StepButtons />
    </div>
  );
};

const StyledSelectTable = withStyles(styles)(SelectTableView);
const SelectTable = transfersCreateConnect(StyledSelectTable);
export default SelectTable;

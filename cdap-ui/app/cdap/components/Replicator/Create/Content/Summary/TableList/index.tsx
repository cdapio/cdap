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
import { createContextConnect, ICreateContext } from 'components/Replicator/Create';
import { generateTableKey, getTableDisplayName } from 'components/Replicator/utilities';
import TextField from '@material-ui/core/TextField';
import InputAdornment from '@material-ui/core/InputAdornment';
import Search from '@material-ui/icons/Search';
import debounce from 'lodash/debounce';
import { List } from 'immutable';
import { ITableImmutable } from 'components/Replicator/types';

const styles = (): StyleRules => {
  return {
    subtitle: {
      marginTop: '10px',
      marginBottom: '10px',
      display: 'grid',
      gridTemplateColumns: '50% 50%',
      alignItems: 'center',

      '& > div:last-child': {
        textAlign: 'right',
      },
    },
    gridWrapper: {
      '& > .grid.grid-container': {
        maxHeight: '500px',
      },
    },
    search: {
      width: '250px',

      '& input': {
        paddingTop: '10px',
        paddingBottom: '10px',
      },
    },
  };
};

const TableListView: React.FC<ICreateContext & WithStyles<typeof styles>> = ({
  classes,
  tables,
  columns,
}) => {
  const [filteredTables, setFilteredTables] = React.useState(tables.toList());
  const [search, setSearch] = React.useState('');

  function handleSearch(e) {
    setSearch(e.target.value);
  }

  const filterTableBySearch = debounce(() => {
    if (!search || search.length === 0) {
      setFilteredTables(tables.toList());
      return;
    }

    const filteredList = tables.toList().filter((row) => {
      const normalizedTable = row.get('table').toLowerCase();
      const normalizedSearch = search.toLowerCase();

      return normalizedTable.indexOf(normalizedSearch) !== -1;
    }) as List<ITableImmutable>;

    setFilteredTables(filteredList);
  }, 300);

  // handle search query change
  React.useEffect(filterTableBySearch, [search]);

  return (
    <div className={classes.root}>
      <div className={classes.subtitle}>
        <div>
          <strong>{tables.size}</strong> tables to be replicated
        </div>
        <div>
          <TextField
            className={classes.search}
            value={search}
            onChange={handleSearch}
            variant="outlined"
            placeholder="Search by table name"
            InputProps={{
              startAdornment: (
                <InputAdornment position="start">
                  <Search />
                </InputAdornment>
              ),
            }}
          />
        </div>
      </div>

      <div className={`grid-wrapper ${classes.gridWrapper}`}>
        <div className="grid grid-container grid-compact">
          <div className="grid-header">
            <div className="grid-row">
              <div>Table name</div>
              <div>Number of columns</div>
            </div>
          </div>

          <div className="grid-body">
            {filteredTables.map((row) => {
              const tableKey = generateTableKey(row);
              const selectedColumns = columns.get(tableKey);

              return (
                <div key={tableKey} className="grid-row">
                  <div>{getTableDisplayName(row)}</div>
                  <div>
                    {selectedColumns && selectedColumns.size > 0 ? selectedColumns.size : 'All'}
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      </div>
    </div>
  );
};

const StyledTableList = withStyles(styles)(TableListView);
const TableList = createContextConnect(StyledTableList);
export default TableList;

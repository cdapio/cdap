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
import { MyReplicatorApi } from 'api/replicator';
import { getCurrentNamespace } from 'services/NamespaceStore';
import { List, Map } from 'immutable';
import Checkbox from '@material-ui/core/Checkbox';
import StepButtons from 'components/Replicator/Create/Content/StepButtons';
import orderBy from 'lodash/orderBy';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import SelectColumns from 'components/Replicator/Create/Content/SelectColumns';
import If from 'components/If';

const styles = (theme): StyleRules => {
  return {
    root: {
      padding: '15px 40px',
    },
    gridWrapper: {
      height: 'calc(100% - 110px - 30px - 35px)',
      '& > .grid.grid-container': {
        maxHeight: '100%', // beating specificity

        '& .grid-row': {
          gridTemplateColumns: '55px 3fr 1fr 1fr 100px',
          alignItems: 'center',
        },

        '&.grid-compact > div[class^="grid-"] .grid-row > div': {
          paddingTop: 0,
          paddingBottom: 0,
        },
      },
    },
    changeLink: {
      color: theme.palette.grey[300],
      cursor: 'pointer',
      '&:hover': {
        textDecoration: 'underline',
        color: theme.palette.blue[200],
      },
    },
  };
};

type ISelectTablesProps = ICreateContext & WithStyles<typeof styles>;

interface ITable {
  database: string;
  table: string;
  numColumns: number;
}

interface IColumn {
  name: string;
  type: string;
}

interface ISelectTablesState {
  tables: ITable[];
  selectedTables: Map<string, boolean>;
  columns: Map<string, List<IColumn>>;
  openTable?: ITable;
  error: any;
  loading: boolean;
}

export function generateTableKey(row) {
  return `db-${row.database}-table-${row.table}`;
}

class SelectTablesView extends React.PureComponent<ISelectTablesProps, ISelectTablesState> {
  public state = {
    tables: [],
    selectedTables: Map<string, boolean>(),
    columns: Map<string, List<IColumn>>(),
    openTable: null,
    loading: true,
    error: null,
  };

  public componentDidMount() {
    this.fetchTables();
  }

  private fetchTables = () => {
    this.setState({
      loading: true,
    });

    const params = {
      namespace: getCurrentNamespace(),
      draftId: this.props.draftId,
    };

    MyReplicatorApi.listTables(params).subscribe(
      (res) => {
        this.setState({ tables: orderBy(res.tables, ['table'], ['asc']) });
      },
      (err) => {
        this.setState({ error: err });
      },
      () => {
        this.setState({ loading: false });
      }
    );
  };

  private toggleSelected = (row) => {
    const key = generateTableKey(row);

    if (this.state.selectedTables.get(key)) {
      this.setState({
        selectedTables: this.state.selectedTables.delete(key),
      });
      return;
    }

    this.setState({
      selectedTables: this.state.selectedTables.set(key, true),
    });
  };

  private toggleSelectAll = () => {
    if (this.state.selectedTables.size > 0) {
      this.setState({
        selectedTables: this.state.selectedTables.clear(),
      });
      return;
    }

    const selectedMap = {};
    this.state.tables.forEach((row) => {
      const key = generateTableKey(row);
      selectedMap[key] = true;
    });

    this.setState({
      selectedTables: Map(selectedMap),
    });
  };

  public openTable = (table = null) => {
    this.setState({
      openTable: table,
    });
  };

  private getInitialSelected = () => {
    if (!this.state.openTable) {
      return null;
    }

    return this.state.columns.get(generateTableKey(this.state.openTable));
  };

  public onColumnsSelection = (tableKey, columns: List<IColumn>) => {
    this.setState({
      columns: this.state.columns.set(tableKey, columns),
    });
  };

  public render() {
    if (this.state.loading) {
      return <LoadingSVGCentered />;
    }

    const { classes } = this.props;

    return (
      <React.Fragment>
        <div className={classes.root}>
          <h3>Select tables and columns to replicate</h3>

          <div className={`grid-wrapper ${classes.gridWrapper}`}>
            <div className={`grid grid-container grid-compact`}>
              <div className="grid-header">
                <div className="grid-row">
                  <div>
                    <Checkbox
                      checked={this.state.selectedTables.size === this.state.tables.length}
                      onChange={this.toggleSelectAll}
                      color="primary"
                    />
                  </div>
                  <div>Table name</div>
                  <div>Total columns</div>
                  <div>Selected columns</div>
                  <div />
                </div>
              </div>

              <div className="grid-body">
                {this.state.tables.map((row) => {
                  const key = generateTableKey(row);
                  const checked = !!this.state.selectedTables.get(key);
                  const columns = this.state.columns.get(key);

                  return (
                    <div key={key} className="grid-row">
                      <div>
                        <Checkbox
                          checked={checked}
                          onChange={this.toggleSelected.bind(this, row)}
                          color="primary"
                        />
                      </div>
                      <div>{row.table}</div>
                      <div>{row.numColumns}</div>
                      <div>{columns ? columns.size : 'All'}</div>
                      <div>
                        <span
                          onClick={this.openTable.bind(this, row)}
                          className={classes.changeLink}
                        >
                          Change
                        </span>
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          </div>

          <StepButtons />
        </div>

        <If condition={this.state.openTable}>
          <SelectColumns
            tableInfo={this.state.openTable}
            initialSelected={this.getInitialSelected()}
            toggle={this.openTable.bind(this, null)}
            onSave={this.onColumnsSelection}
          />
        </If>
      </React.Fragment>
    );
  }
}

const StyledSelectTables = withStyles(styles)(SelectTablesView);
const SelectTables = createContextConnect(StyledSelectTables);
export default SelectTables;

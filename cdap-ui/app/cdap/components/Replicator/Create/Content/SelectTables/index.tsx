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
import { List, Map, Set, fromJS } from 'immutable';
import Checkbox from '@material-ui/core/Checkbox';
import StepButtons from 'components/Replicator/Create/Content/StepButtons';
import orderBy from 'lodash/orderBy';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import SelectColumns from 'components/Replicator/Create/Content/SelectColumns';
import If from 'components/If';
import { generateTableKey, extractErrorMessage } from 'components/Replicator/utilities';
import FormControlLabel from '@material-ui/core/FormControlLabel';
import Heading, { HeadingTypes } from 'components/Heading';
import ManualSelectTable from 'components/Replicator/Create/Content/SelectTables/ManualSelectTable';

const styles = (theme): StyleRules => {
  return {
    root: {
      padding: '15px 40px',
    },
    gridWrapper: {
      height: 'calc(100% - 110px - 30px - 35px)',
      '& > .grid.grid-container': {
        maxHeight: '100%', // beating specificity

        '& .grid-header': {
          zIndex: 4,
        },

        '& .grid-row': {
          gridTemplateColumns: '55px 3fr 1fr 1fr 200px 120px 120px 120px',
          alignItems: 'center',
        },

        '&.grid-compact > div[class^="grid-"] .grid-row > div': {
          paddingTop: 0,
          paddingBottom: 0,
        },
      },
    },
    changeLink: {
      color: theme.palette.grey[200],
      cursor: 'pointer',
      '&:hover': {
        textDecoration: 'underline',
        color: theme.palette.blue[200],
      },
    },
    headerDMLcheckbox: {
      marginLeft: '-11px',
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

export enum DML {
  insert = 'INSERT',
  update = 'UPDATE',
  delete = 'DELETE',
}

interface ISelectTablesState {
  tables: ITable[];
  selectedTables: Map<string, Map<string, string>>;
  columns: Map<string, List<IColumn>>;
  dmlBlacklist: Map<string, Set<DML>>;
  openTable?: ITable;
  error: any;
  loading: boolean;
}

class SelectTablesView extends React.PureComponent<ISelectTablesProps, ISelectTablesState> {
  public state = {
    tables: [],
    selectedTables: this.props.tables,
    columns: this.props.columns,
    dmlBlacklist: this.props.dmlBlacklist,
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
        this.setState({ tables: orderBy(res.tables, ['table'], ['asc']), loading: false });
      },
      (err) => {
        // tslint:disable-next-line: no-console
        console.log('error', err);
        this.setState({ error: extractErrorMessage(err), loading: false });
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
      selectedTables: this.state.selectedTables.set(key, fromJS(row)),
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
      selectedMap[key] = fromJS(row);
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

  /**
   * returns
   *  -1 for none selected
   *  0 for indeterminate
   *  1 for all selected
   */
  private dmlSelectAllIndicator = (dmlEvent: DML) => {
    const dmlListArray = this.state.tables.map((table) => {
      const key = generateTableKey(table);
      const tableDML = this.state.dmlBlacklist.get(key);

      if (!tableDML) {
        return false;
      }

      return tableDML.has(dmlEvent);
    });

    // creating a set will create a unique value array
    const dmlListSet = Set(dmlListArray);

    if (dmlListSet.size === 2) {
      return 0;
    }

    return dmlListSet.has(false) ? 1 : -1;
  };

  private toggleAllDML = (dmlEvent: DML) => {
    let dmlBlacklist = this.state.dmlBlacklist;

    if (this.dmlSelectAllIndicator(dmlEvent) !== -1) {
      // deselect all
      this.state.tables.forEach((table) => {
        const key = generateTableKey(table);
        let tableDML = dmlBlacklist.get(key);

        if (!tableDML) {
          tableDML = Set<DML>([dmlEvent]);
        } else {
          tableDML = tableDML.add(dmlEvent);
        }

        dmlBlacklist = dmlBlacklist.set(key, tableDML);
      });
    } else {
      // select all
      this.state.tables.forEach((table) => {
        const key = generateTableKey(table);
        let tableDML = dmlBlacklist.get(key);

        if (!tableDML) {
          tableDML = Set<DML>();
        } else {
          tableDML = tableDML.remove(dmlEvent);
        }

        dmlBlacklist = dmlBlacklist.set(key, tableDML);
      });
    }

    this.setState({
      dmlBlacklist,
    });
  };

  private toggleDML = (key, dmlEvent: DML) => {
    // get current blacklist
    const tableDML = this.state.dmlBlacklist.get(key);
    let dmlSet: Set<DML>;
    if (!tableDML) {
      dmlSet = Set<DML>([dmlEvent]);
    } else if (tableDML.has(dmlEvent)) {
      dmlSet = tableDML.delete(dmlEvent);
    } else {
      dmlSet = tableDML.add(dmlEvent);
    }

    this.setState({
      dmlBlacklist: this.state.dmlBlacklist.set(key, dmlSet),
    });
  };

  private renderError = () => {
    if (!this.state.error) {
      return null;
    }

    return (
      <React.Fragment>
        <br />
        <div className="text-danger">
          <Heading type={HeadingTypes.h5} label="Error" />
          <span>{this.state.error}</span>
        </div>
        <ManualSelectTable />
      </React.Fragment>
    );
  };

  public handleNext = () => {
    this.props.setTables(this.state.selectedTables, this.state.columns, this.state.dmlBlacklist);
  };

  private renderContent = () => {
    if (this.state.error) {
      return null;
    }

    const { classes } = this.props;

    return (
      <React.Fragment>
        <div className={`grid-wrapper ${classes.gridWrapper}`}>
          <div className={`grid grid-container grid-compact`}>
            <div className="grid-header">
              <div className="grid-row">
                <div>
                  <Checkbox
                    checked={this.state.selectedTables.size === this.state.tables.length}
                    indeterminate={
                      this.state.selectedTables.size < this.state.tables.length &&
                      this.state.selectedTables.size > 0
                    }
                    onChange={this.toggleSelectAll}
                    color="primary"
                  />
                </div>
                <div>Table name</div>
                <div>Total columns</div>
                <div>Selected columns</div>
                <div />
                <div>
                  <Checkbox
                    color="primary"
                    className={classes.headerDMLcheckbox}
                    checked={this.dmlSelectAllIndicator(DML.insert) === 1}
                    indeterminate={this.dmlSelectAllIndicator(DML.insert) === 0}
                    onChange={this.toggleAllDML.bind(this, DML.insert)}
                  />
                </div>
                <div>
                  <Checkbox
                    color="primary"
                    className={classes.headerDMLcheckbox}
                    checked={this.dmlSelectAllIndicator(DML.update) === 1}
                    indeterminate={this.dmlSelectAllIndicator(DML.update) === 0}
                    onChange={this.toggleAllDML.bind(this, DML.update)}
                  />
                </div>
                <div>
                  <Checkbox
                    color="primary"
                    className={classes.headerDMLcheckbox}
                    checked={this.dmlSelectAllIndicator(DML.delete) === 1}
                    indeterminate={this.dmlSelectAllIndicator(DML.delete) === 0}
                    onChange={this.toggleAllDML.bind(this, DML.delete)}
                  />
                </div>
              </div>
            </div>

            <div className="grid-body">
              {this.state.tables.map((row) => {
                const key = generateTableKey(row);
                const checked = !!this.state.selectedTables.get(key);
                const columns = this.state.columns.get(key);
                const tableDML = this.state.dmlBlacklist.get(key) || Set<DML>();

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
                      <span onClick={this.openTable.bind(this, row)} className={classes.changeLink}>
                        Change
                      </span>
                    </div>
                    <div>
                      <FormControlLabel
                        control={
                          <Checkbox
                            color="primary"
                            onClick={this.toggleDML.bind(this, key, DML.insert)}
                            checked={!tableDML.has(DML.insert)}
                          />
                        }
                        label="Inserts"
                      />
                    </div>
                    <div>
                      <FormControlLabel
                        control={
                          <Checkbox
                            color="primary"
                            onClick={this.toggleDML.bind(this, key, DML.update)}
                            checked={!tableDML.has(DML.update)}
                          />
                        }
                        label="Updates"
                      />
                    </div>
                    <div>
                      <FormControlLabel
                        control={
                          <Checkbox
                            color="primary"
                            onClick={this.toggleDML.bind(this, key, DML.delete)}
                            checked={!tableDML.has(DML.delete)}
                          />
                        }
                        label="Deletes"
                      />
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        </div>
        <StepButtons onNext={this.handleNext} />
      </React.Fragment>
    );
  };

  public render() {
    if (this.state.loading) {
      return <LoadingSVGCentered />;
    }

    const { classes } = this.props;

    return (
      <React.Fragment>
        <div className={classes.root}>
          <Heading type={HeadingTypes.h3} label="Select tables and columns to replicate" />

          {this.renderError()}
          {this.renderContent()}
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

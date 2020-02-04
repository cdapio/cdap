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
import { createContextConnect } from 'components/Replicator/Create';
import { List, Map } from 'immutable';
import Button from '@material-ui/core/Button';
import CloseIcon from '@material-ui/icons/Close';
import IconButton from '@material-ui/core/IconButton';
import { getCurrentNamespace } from 'services/NamespaceStore';
import { MyReplicatorApi } from 'api/replicator';
import Checkbox from '@material-ui/core/Checkbox';
import { generateTableKey } from 'components/Replicator/Create/Content/SelectTables';
import LoadingSVG from 'components/LoadingSVG';

const styles = (theme): StyleRules => {
  return {
    root: {
      position: 'absolute',
      top: '100px',
      right: 0,
      height: 'calc(100vh - 100px - 70px)',
      width: '750px',
      padding: '15px 30px',
      backgroundColor: theme.palette.white[50],
      border: `1px solid ${theme.palette.grey[200]}`,
      boxShadow: `0 3px 10px 1px ${theme.palette.grey[200]}`,
    },
    header: {
      display: 'grid',
      gridTemplateColumns: '75% 25%',
    },
    actionButtons: {
      textAlign: 'right',
      '& > button:not(:last-child)': {
        marginRight: '10px',
      },
    },
    gridWrapper: {
      maxHeight: 'calc(100% - 75px - 30px)',
      '& .grid.grid-container.grid-compact': {
        height: '100%',

        '& .grid-row': {
          gridTemplateColumns: '55px 40px 1fr 200px 55px 100px',
          alignItems: 'center',
        },

        '& > div[class^="grid-"] .grid-row > div': {
          paddingTop: 0,
          paddingBottom: 0,
        },
      },
    },
    nullableCheckbox: {
      paddingLeft: 0,
    },
    loadingContainer: {
      textAlign: 'center',
      marginTop: '100px',
    },
  };
};

interface ISelectColumnsProps extends WithStyles<typeof styles> {
  tableInfo?: {
    table: string;
    database: string;
  };
  onSave: (tableKey, columns) => void;
  initialSelected: List<Map<string, string>>;
  toggle: () => void;
  draftId: string;
}

interface IColumn {
  name: string;
  type: string;
  nullable: boolean;
}

interface ISelectColumnsState {
  columns: IColumn[];
  primaryKeys: string[];
  selectedColumns: Map<string, Map<string, string>>;
  loading: boolean;
  error: any;
}

class SelectColumnsView extends React.PureComponent<ISelectColumnsProps, ISelectColumnsState> {
  public state = {
    columns: [],
    primaryKeys: [],
    selectedColumns: Map<string, Map<string, string>>(),
    loading: true,
    error: null,
  };

  public componentDidMount() {
    this.fetchColumns();
  }

  private fetchColumns = () => {
    this.setState({
      loading: true,
    });

    const params = {
      namespace: getCurrentNamespace(),
      draftId: this.props.draftId,
    };

    const body = {
      table: this.props.tableInfo.table,
      database: this.props.tableInfo.database,
    };

    MyReplicatorApi.getTableInfo(params, body).subscribe(
      (res) => {
        const selectedColumns = {};
        if (this.props.initialSelected && this.props.initialSelected.size > 0) {
          this.props.initialSelected.forEach((row) => {
            selectedColumns[row.get('name')] = row;
          });
        }

        this.setState({
          columns: res.columns,
          primaryKeys: res.primaryKey,
          selectedColumns: Map(selectedColumns),
        });
      },
      (err) => {
        this.setState({ error: err });
      },
      () => {
        this.setState({
          loading: false,
        });
      }
    );
  };

  private handleSave = () => {
    const selectedList = this.state.selectedColumns.toList();
    const tableKey = generateTableKey(this.props.tableInfo);

    this.props.onSave(tableKey, selectedList);
    this.props.toggle();
  };

  private toggleSelected = (row) => {
    const key = row.name;
    if (this.state.selectedColumns.get(key)) {
      this.setState({
        selectedColumns: this.state.selectedColumns.delete(key),
      });
      return;
    }

    this.setState({
      selectedColumns: this.state.selectedColumns.set(key, Map({ name: row.name, type: row.type })),
    });
  };

  private toggleSelectAll = () => {
    if (this.state.selectedColumns.size > 0) {
      this.setState({
        selectedColumns: this.state.selectedColumns.clear(),
      });
      return;
    }

    const selectedMap = {};
    this.state.columns.forEach((row) => {
      selectedMap[row.name] = Map({ name: row.name, type: row.type });
    });

    this.setState({
      selectedColumns: Map(selectedMap),
    });
  };

  private renderContent = () => {
    const { classes } = this.props;

    return (
      <div className={`grid-wrapper ${classes.gridWrapper}`}>
        <div className="grid grid-container grid-compact">
          <div className="grid-header">
            <div className="grid-row">
              <div>
                <Checkbox
                  color="primary"
                  checked={this.state.selectedColumns.size === this.state.columns.length}
                  onChange={this.toggleSelectAll}
                />
              </div>
              <div>#</div>
              <div>Column name</div>
              <div>Type</div>
              <div>Null</div>
              <div>Key</div>
            </div>
          </div>

          <div className="grid-body">
            {this.state.columns.map((row, i) => {
              return (
                <div key={row.name} className="grid-row">
                  <div>
                    <Checkbox
                      color="primary"
                      checked={!!this.state.selectedColumns.get(row.name)}
                      onChange={this.toggleSelected.bind(this, row)}
                    />
                  </div>
                  <div>{i + 1}</div>
                  <div>{row.name}</div>
                  <div>{row.type}</div>
                  <div>
                    <Checkbox
                      checked={row.nullable}
                      disabled={true}
                      className={classes.nullableCheckbox}
                    />
                  </div>
                  <div>{this.state.primaryKeys.indexOf(row.name) !== -1 ? 'Primary' : '--'}</div>
                </div>
              );
            })}
          </div>
        </div>
      </div>
    );
  };

  private renderLoading = () => {
    return (
      <div className={this.props.classes.loadingContainer}>
        <LoadingSVG />
      </div>
    );
  };

  public render() {
    if (!this.props.tableInfo) {
      return null;
    }

    const { classes } = this.props;

    return (
      <div className={classes.root}>
        <div className={classes.header}>
          <div>
            <h3>{this.props.tableInfo.table}</h3>
            <div>Select the columns to be replicated</div>
            <div>
              Columns - {this.state.selectedColumns.size} of {this.state.columns.length} selected
            </div>
          </div>

          <div className={classes.actionButtons}>
            <Button
              variant="contained"
              color="primary"
              onClick={this.handleSave}
              disabled={this.state.loading}
            >
              Save
            </Button>

            <IconButton onClick={this.props.toggle}>
              <CloseIcon />
            </IconButton>
          </div>
        </div>

        {this.state.loading ? this.renderLoading() : this.renderContent()}
      </div>
    );
  }
}

const StyledSelectColumns = withStyles(styles)(SelectColumnsView);
const SelectColumns = createContextConnect(StyledSelectColumns);
export default SelectColumns;

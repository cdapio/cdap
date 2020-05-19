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
import { generateTableKey } from 'components/Replicator/utilities';
import LoadingSVG from 'components/LoadingSVG';
import Heading, { HeadingTypes } from 'components/Heading';
import FormControlLabel from '@material-ui/core/FormControlLabel';
import Radio from '@material-ui/core/Radio';
import RadioGroup from '@material-ui/core/RadioGroup';
import If from 'components/If';
import SearchBox from 'components/Replicator/Create/Content/SearchBox';
import debounce from 'lodash/debounce';

const styles = (theme): StyleRules => {
  return {
    backdrop: {
      position: 'absolute',
      top: 0,
      left: 0,
      right: 0,
      bottom: 0,
      backgroundColor: 'rgba(0, 0, 0, 0.1)',
      zIndex: 5,
    },
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
      // 100% - heading section - subtitle/search box
      height: 'calc(100% - 100px - 40px)',
      '& .grid.grid-container.grid-compact': {
        maxHeight: '100%',

        '& .grid-header': {
          zIndex: 5,
        },

        '& .grid-row': {
          gridTemplateColumns: '40px 40px 1fr 200px 55px 100px',
          alignItems: 'center',
        },

        '& > div[class^="grid-"] .grid-row > div': {
          paddingTop: 0,
          paddingBottom: 0,
        },
      },
    },
    loadingContainer: {
      textAlign: 'center',
      marginTop: '100px',
    },
    replicateSelectionRadio: {
      marginRight: '10px',
    },
    radioContainer: {
      paddingLeft: '10px',
      marginTop: '15px',
      marginBottom: '5px',
    },
    radio: {
      padding: 0,
    },
    subtitleContainer: {
      display: 'flex',
      alignItems: 'center',
      marginBottom: '10px',

      '& > div': {
        marginRight: '25px',
      },
    },
    overlay: {
      backgroundColor: theme.palette.white[50],
      opacity: 0.7,
      top: '100px',
      bottom: 0,
      left: 0,
      right: 0,
      position: 'absolute',
      zIndex: 6, // to beat grid-header z-index
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
enum ReplicateSelect {
  all = 'ALL',
  individual = 'INDIVIDUAL',
}

interface ISelectColumnsState {
  columns: IColumn[];
  filteredColumns: IColumn[];
  primaryKeys: string[];
  selectedReplication: ReplicateSelect;
  selectedColumns: Map<string, Map<string, string>>;
  loading: boolean;
  error: any;
  search: string;
}

class SelectColumnsView extends React.PureComponent<ISelectColumnsProps, ISelectColumnsState> {
  public state = {
    columns: [],
    filteredColumns: [],
    primaryKeys: [],
    selectedReplication: ReplicateSelect.all,
    selectedColumns: Map<string, Map<string, string>>(),
    loading: true,
    error: null,
    search: '',
  };

  public componentDidMount() {
    this.fetchColumns();
  }

  private getInitialSelectedColumns = (columns): Map<string, Map<string, string>> => {
    const existingColumns = {};
    columns.forEach((column) => {
      existingColumns[column.name] = true;
    });

    let hasChange = false;

    const selectedColumns = {};
    if (this.props.initialSelected && this.props.initialSelected.size > 0) {
      this.props.initialSelected.forEach((row) => {
        if (existingColumns[row.get('name')]) {
          selectedColumns[row.get('name')] = row;
        } else {
          hasChange = true;
        }
      });
    }

    const response: Map<string, Map<string, string>> = Map(selectedColumns);

    if (hasChange) {
      const tableKey = generateTableKey(this.props.tableInfo);
      this.props.onSave(tableKey, response.toList());
    }

    return response;
  };

  private setFilteredColumns = debounce(
    (search = this.state.search, columns = this.state.columns) => {
      let filteredColumns = columns;
      if (search && search.length > 0) {
        filteredColumns = filteredColumns.filter((row) => {
          const normalizedColumn = row.name.toLowerCase();
          const normalizedSearch = search.toLowerCase();

          return normalizedColumn.indexOf(normalizedSearch) !== -1;
        });
      }

      this.setState({ filteredColumns });
    },
    300
  );

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
        const selectedColumns = this.getInitialSelectedColumns(res.columns);

        this.setState(
          {
            columns: res.columns,
            primaryKeys: res.primaryKey,
            selectedColumns,
            selectedReplication:
              selectedColumns.size === 0 ? ReplicateSelect.all : ReplicateSelect.individual,
          },
          () => {
            if (selectedColumns.size === 0) {
              this.toggleSelectAll();
            }

            this.setFilteredColumns();
          }
        );
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

  private handleSearch = (search) => {
    this.setState({ search });
    this.setFilteredColumns(search);
  };

  private handleSave = () => {
    const tableKey = generateTableKey(this.props.tableInfo);
    const selectedList =
      this.state.selectedReplication === ReplicateSelect.all
        ? null
        : this.state.selectedColumns.toList();

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

  private handleReplicationSelection = (e) => {
    this.setState({
      selectedReplication: e.target.value,
    });
  };

  private renderContent = () => {
    const { classes } = this.props;

    return (
      <React.Fragment>
        <If condition={this.state.selectedReplication === ReplicateSelect.all}>
          <div className={classes.overlay} />
        </If>
        <div className={classes.subtitleContainer}>
          <div>
            {`Columns - ${this.state.selectedColumns.size} of ${this.state.columns.length} selected`}
          </div>

          <div>
            <SearchBox
              value={this.state.search}
              onChange={this.handleSearch}
              placeholder="Search by column name"
            />
          </div>
        </div>
        <div className={`grid-wrapper ${classes.gridWrapper}`}>
          <div className="grid grid-container grid-compact">
            <div className="grid-header">
              <div className="grid-row">
                <div>
                  <Checkbox
                    color="primary"
                    className={classes.radio}
                    checked={this.state.selectedColumns.size === this.state.columns.length}
                    indeterminate={
                      this.state.selectedColumns.size < this.state.columns.length &&
                      this.state.selectedColumns.size > 0
                    }
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
              {this.state.filteredColumns.map((row, i) => {
                return (
                  <div key={row.name} className="grid-row">
                    <div>
                      <Checkbox
                        color="primary"
                        className={classes.radio}
                        checked={!!this.state.selectedColumns.get(row.name)}
                        onChange={this.toggleSelected.bind(this, row)}
                      />
                    </div>
                    <div>{i + 1}</div>
                    <div>{row.name}</div>
                    <div>{row.type}</div>
                    <div>
                      <Checkbox className={classes.radio} checked={row.nullable} disabled={true} />
                    </div>
                    <div>{this.state.primaryKeys.indexOf(row.name) !== -1 ? 'Primary' : '--'}</div>
                  </div>
                );
              })}
            </div>
          </div>
        </div>
      </React.Fragment>
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
      <div className={classes.backdrop}>
        <div className={classes.root}>
          <div className={classes.header}>
            <div>
              <Heading type={HeadingTypes.h3} label={this.props.tableInfo.table} />
              <If condition={!this.state.loading}>
                <div className={classes.radioContainer}>
                  <RadioGroup
                    value={this.state.selectedReplication}
                    onChange={this.handleReplicationSelection}
                    className={classes.radioGroup}
                  >
                    <FormControlLabel
                      value={ReplicateSelect.all}
                      control={
                        <Radio
                          color="primary"
                          className={`${classes.radio} ${classes.replicateSelectionRadio}`}
                        />
                      }
                      label="Replicate all columns available"
                    />
                    <FormControlLabel
                      value={ReplicateSelect.individual}
                      control={
                        <Radio
                          color="primary"
                          className={`${classes.radio} ${classes.replicateSelectionRadio}`}
                        />
                      }
                      label="Select the columns to be replicated"
                    />
                  </RadioGroup>
                </div>
              </If>
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
      </div>
    );
  }
}

const StyledSelectColumns = withStyles(styles)(SelectColumnsView);
const SelectColumns = createContextConnect(StyledSelectColumns);
export default SelectColumns;

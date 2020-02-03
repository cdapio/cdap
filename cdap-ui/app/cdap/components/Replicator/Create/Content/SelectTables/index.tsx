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
import { Map } from 'immutable';
import Checkbox from '@material-ui/core/Checkbox';
import StepButtons from 'components/Replicator/Create/Content/StepButtons';
import orderBy from 'lodash/orderBy';
import LoadingSVGCentered from 'components/LoadingSVGCentered';

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
          gridTemplateColumns: '55px 3fr 1fr 100px',
          alignItems: 'center',
        },

        '&.grid-compact > div[class^="grid-"] .grid-row > div': {
          paddingTop: 0,
          paddingBottom: 0,
        },
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

interface ISelectTablesState {
  tables: ITable[];
  selectedTables: Map<string, boolean>;
  error: any;
  loading: boolean;
}

class SelectTablesView extends React.PureComponent<ISelectTablesProps, ISelectTablesState> {
  public state = {
    tables: [],
    selectedTables: Map<string, boolean>(),
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

  private generateKey = (row) => {
    return `db-${row.database}-table-${row.table}`;
  };

  private toggleSelected = (row) => {
    const key = this.generateKey(row);

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
      const key = this.generateKey(row);
      selectedMap[key] = true;
    });

    this.setState({
      selectedTables: this.state.selectedTables.merge(Map(selectedMap)),
    });
  };

  public render() {
    if (this.state.loading) {
      return <LoadingSVGCentered />;
    }

    const { classes } = this.props;

    return (
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
                <div />
              </div>
            </div>

            <div className="grid-body">
              {this.state.tables.map((row) => {
                const key = this.generateKey(row);
                const checked = !!this.state.selectedTables.get(key);

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
                    <div>Change</div>
                  </div>
                );
              })}
            </div>
          </div>
        </div>

        <StepButtons />
      </div>
    );
  }
}

const StyledSelectTables = withStyles(styles)(SelectTablesView);
const SelectTables = createContextConnect(StyledSelectTables);
export default SelectTables;

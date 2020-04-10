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
import Input from '@material-ui/core/Input';
import withStyles, { StyleRules } from '@material-ui/core/styles/withStyles';
import AbstractRow, {
  IAbstractRowProps,
  AbstractRowStyles,
} from 'components/AbstractWidget/AbstractMultiRowWidget/AbstractRow';
import Checkbox from '@material-ui/core/Checkbox';
import FormControlLabel from '@material-ui/core/FormControlLabel';
import { Set } from 'immutable';
import { DML } from 'components/Replicator/Create/Content/SelectTables';
import { objectQuery } from 'services/helpers';
import ColumnsMultiRow from '../ColumnsMultiRow';
import Heading, { HeadingTypes } from 'components/Heading';
import Radio from '@material-ui/core/Radio';
import RadioGroup from '@material-ui/core/RadioGroup';
import If from 'components/If';
import classnames from 'classnames';

const styles = (theme): StyleRules => {
  return {
    ...AbstractRowStyles(theme),
    root: {
      ...AbstractRowStyles(theme).root,
      height: 'initial',
    },
    disabled: {
      color: `${theme.palette.grey['50']}`,
    },
    inputContainer: {
      display: 'grid',
      gridTemplateColumns: '1fr 100px 100px 100px',
    },
    inputBox: {
      marginRight: '15px',
    },
    checkbox: {
      '& label': {
        marginBottom: 0,
      },
    },
    columnsSelectorContainer: {
      padding: '5px 0 15px 40px',
      width: '60%',
      minWidth: '500px',
      marginBottom: '15px',
    },
    border: {
      borderLeft: `3px solid ${theme.palette.grey[500]}`,
    },
    radioGroup: {
      flexDirection: 'initial',
      marginBottom: '15px',
      '& > label': {
        marginRight: '40px',
      },
    },
  };
};

interface ITableRowProps extends IAbstractRowProps<typeof styles> {
  value: {
    table: string;
    dmlBlacklist: Set<DML>;
    columns: string[];
  };
}

enum ColumnsSelect {
  all = 'ALL',
  manual = 'MANUAL',
}

interface ITableRowState {
  table: string;
  dmlBlacklist: Set<DML>;
  columns: string[];
  selectColumn: ColumnsSelect;
}

class TableRowView extends AbstractRow<ITableRowProps, ITableRowState> {
  public state = {
    table: objectQuery(this.props, 'value', 'table') || '',
    dmlBlacklist: objectQuery(this.props, 'value', 'dmlBlacklist') || Set<DML>(),
    columns: objectQuery(this.props, 'value', 'columns') || [],
    selectColumn:
      (objectQuery(this.props, 'value', 'columns') || []).length > 0
        ? ColumnsSelect.manual
        : ColumnsSelect.all,
  };

  public componentDidMount() {
    this.setState({
      table: objectQuery(this.props, 'value', 'table') || '',
      dmlBlacklist: objectQuery(this.props, 'value', 'dmlBlacklist') || Set<DML>(),
      columns: objectQuery(this.props, 'value', 'columns') || [],
      selectColumn:
        (objectQuery(this.props, 'value', 'columns') || []).length > 0
          ? ColumnsSelect.manual
          : ColumnsSelect.all,
    });
  }

  private constructValue = () => {
    const columns = this.state.selectColumn === ColumnsSelect.all ? [] : this.state.columns;

    this.onChange({
      table: this.state.table,
      dmlBlacklist: this.state.dmlBlacklist,
      columns,
    });
  };

  private handleTableChange = (e) => {
    const table = e.target.value;

    this.setState(
      {
        table,
      },
      this.constructValue
    );
  };

  private toggleDML = (dmlEvent) => {
    const tableDML = this.state.dmlBlacklist;
    let dmlSet: Set<DML>;
    if (!tableDML) {
      dmlSet = Set<DML>([dmlEvent]);
    } else if (tableDML.has(dmlEvent)) {
      dmlSet = tableDML.delete(dmlEvent);
    } else {
      dmlSet = tableDML.add(dmlEvent);
    }

    this.setState(
      {
        dmlBlacklist: dmlSet,
      },
      this.constructValue
    );
  };

  private handleColumnChange = (columns) => {
    this.setState(
      {
        columns,
      },
      this.constructValue
    );
  };

  private handleSelectColumnChange = (e) => {
    this.setState(
      {
        selectColumn: e.target.value,
      },
      this.constructValue
    );
  };

  public renderInput = () => {
    const { classes } = this.props;

    return (
      <div>
        <div className={classes.inputContainer}>
          <Input
            placeholder="Table name"
            onChange={this.handleTableChange}
            value={this.state.table}
            autoFocus={this.props.autofocus}
            onKeyPress={this.handleKeyPress}
            onKeyDown={this.handleKeyDown}
            inputRef={this.props.forwardedRef}
            className={classes.inputBox}
          />

          <div>
            <FormControlLabel
              control={
                <Checkbox
                  color="primary"
                  onClick={this.toggleDML.bind(this, DML.insert)}
                  checked={!this.state.dmlBlacklist.has(DML.insert)}
                  className={classes.checkbox}
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
                  onClick={this.toggleDML.bind(this, DML.update)}
                  checked={!this.state.dmlBlacklist.has(DML.update)}
                  className={classes.checkbox}
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
                  onClick={this.toggleDML.bind(this, DML.delete)}
                  checked={!this.state.dmlBlacklist.has(DML.delete)}
                  className={classes.checkbox}
                />
              }
              label="Deletes"
            />
          </div>
        </div>
        <div
          className={classnames(classes.columnsSelectorContainer, {
            [classes.border]: this.state.selectColumn === ColumnsSelect.manual,
          })}
        >
          <div>
            <RadioGroup
              value={this.state.selectColumn}
              onChange={this.handleSelectColumnChange}
              className={classes.radioGroup}
            >
              <FormControlLabel
                value={ColumnsSelect.all}
                control={<Radio />}
                label="Replicate all columns"
              />
              <FormControlLabel
                value={ColumnsSelect.manual}
                control={<Radio />}
                label="Manual entry"
              />
            </RadioGroup>
          </div>
          <If condition={this.state.selectColumn === ColumnsSelect.manual}>
            <div>
              <Heading type={HeadingTypes.h5} label="Columns" />
              <ColumnsMultiRow value={this.state.columns} onChange={this.handleColumnChange} />
            </div>
          </If>
        </div>
      </div>
    );
  };
}

const TableRow = withStyles(styles)(TableRowView);
export default TableRow;

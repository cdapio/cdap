/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the 'License'); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an 'AS IS' BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import React from 'react';
import PropTypes from 'prop-types';
import classnames from 'classnames';

import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import ExpansionPanel from '@material-ui/core/ExpansionPanel';
import ExpansionPanelSummary from '@material-ui/core/ExpansionPanelSummary';
import ExpansionPanelDetails from '@material-ui/core/ExpansionPanelDetails';
import ExpandMoreIcon from '@material-ui/icons/ExpandMore';
import Table from '@material-ui/core/Table';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import TableHead from '@material-ui/core/TableHead';
import TableRow from '@material-ui/core/TableRow';
import Menu from '@material-ui/core/Menu';
import MenuItem from '@material-ui/core/MenuItem';
import Button from '@material-ui/core/Button';
import Typography from '@material-ui/core/Typography';

import FieldRow from 'components/AbstractWidget/SqlSelectorWidget/FieldRow';
import If from 'components/If';

import { IParsedInputSchema } from 'components/AbstractWidget/SqlSelectorWidget';

const styles = (theme) => {
  return {
    schemaContainer: {
      margin: '5px 0px',
    },
    schemaTable: {
      overflow: 'hidden',
    },
    schemaTableHeader: {
      fontSize: '13px',
      borderBottom: '0px',
    },
    tableHeaderSelectIcon: {
      marginLeft: '5px',
    },
    tableRowTopBorder: {
      borderTop: `1px solid ${theme.palette.grey['400']}`,
    },
    badgeDanger: {
      backgroundColor: '#ff6666',
      color: 'white',
      fontSize: '9px',
      marginLeft: '5px',
    },
    errorRow: {
      '& td': {
        border: '0px',
      },
      backgroundColor: '#fde2e2',
      border: '1px solid #ff6666',
    },
  };
};

interface ISchemaContainerProps extends WithStyles<typeof styles> {
  stage: IParsedInputSchema;
  onExpandClick: (arg0: any) => void;
  onSchemaChange: (arg0: any) => void;
  aliases: object;
  errorCount: number;
}

interface ISchemaContainerState {
  selectFields: string;
  menuAnchor: any;
}

class SchemaContainer extends React.Component<ISchemaContainerProps, ISchemaContainerState> {
  public state = {
    selectFields: 'All',
    menuAnchor: null,
  };

  public tableSelectChange = (selectFields) => {
    this.setState({ selectFields }, () => {
      const newSchema = this.props.stage.schema.map((field) => {
        return {
          ...field,
          selected: this.state.selectFields === 'All' ? true : false,
        };
      });
      this.props.onSchemaChange(this.constructNewStage(newSchema));
    });
  };

  public handleMenuClose = (value) => {
    this.setState({ menuAnchor: null }, () => {
      if (value) {
        this.tableSelectChange(value);
      }
    });
  };

  public handleMenuOpen = (event) => {
    this.setState({ menuAnchor: event.currentTarget });
  };

  public constructNewStage = (newSchema) => {
    return {
      ...this.props.stage,
      schema: newSchema,
    };
  };

  public toggleExpansionPanel = () => {
    this.props.onExpandClick(this.props.stage);
  };

  public updateField = (newField) => {
    if (this.props.stage && this.props.stage.schema) {
      const newSchema = this.props.stage.schema.map((field) => {
        if (field.name === newField.name) {
          return newField;
        }
        return field;
      });
      this.props.onSchemaChange(this.constructNewStage(newSchema));
    }
  };

  public render() {
    const { classes } = this.props;
    return (
      <ExpansionPanel
        className={classes.schemaContainer}
        expanded={this.props.stage.expanded}
        onChange={this.toggleExpansionPanel}
      >
        <ExpansionPanelSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="h6">{this.props.stage.name}</Typography>
          <If condition={Boolean(this.props.errorCount)}>
            <span className={classnames('badge', classes.badgeDanger)}>
              {this.props.errorCount}
            </span>
          </If>
        </ExpansionPanelSummary>
        <ExpansionPanelDetails>
          <Table className={classes.schemaTable}>
            <TableHead>
              <TableRow>
                <TableCell className={classes.schemaTableHeader} align="center">
                  <Typography variant="h6">Name</Typography>
                </TableCell>
                <TableCell align="center" className={classes.schemaTableHeader}>
                  <Button size="small" variant="outlined" onClick={this.handleMenuOpen}>
                    <span>Select</span>
                    <span
                      className={classnames('fa', 'fa-chevron-down', classes.tableHeaderSelectIcon)}
                    />
                  </Button>
                  <Menu
                    anchorEl={this.state.menuAnchor}
                    open={Boolean(this.state.menuAnchor)}
                    onClose={this.handleMenuClose}
                  >
                    <MenuItem onClick={() => this.handleMenuClose('All')}>All</MenuItem>
                    <MenuItem onClick={() => this.handleMenuClose('None')}>None</MenuItem>
                  </Menu>
                </TableCell>
                <TableCell align="center" className={classes.schemaTableHeader}>
                  <Typography variant="h6">Alias</Typography>
                </TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {this.props.stage.schema &&
                this.props.stage.schema.map((field, i) => {
                  const aliasError = this.props.aliases && this.props.aliases[field.alias] > 1;
                  // Material UI table styles has border on table head which is
                  // interfering with top border for first row in the table while
                  // we try to display error rows with red border. So, we removed
                  // bottom border on table head and added top border on first
                  // row of the table.
                  return (
                    <TableRow
                      className={classnames({
                        [classes.tableRowTopBorder]: !aliasError && i === 0,
                        [classes.errorRow]: aliasError,
                      })}
                    >
                      <FieldRow field={field} onFieldChange={this.updateField} />
                    </TableRow>
                  );
                })}
            </TableBody>
          </Table>
        </ExpansionPanelDetails>
      </ExpansionPanel>
    );
  }
}

(SchemaContainer as any).propTypes = {
  stage: PropTypes.any,
  aliases: PropTypes.any,
  errorCount: PropTypes.number,
  onExpandClick: PropTypes.func,
  onSchemaChange: PropTypes.func,
};

export default withStyles(styles)(SchemaContainer);

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
import classnames from 'classnames';
import T from 'i18n-react';

import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
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
import IconSVG from 'components/IconSVG';

import { IParsedInputSchema } from 'components/AbstractWidget/SqlSelectorWidget';
import { IErrorObj } from 'components/ConfigurationGroup/utilities';

const I18N_PREFIX_TABLE = 'features.AbstractWidget.SqlSelectorWidget.table';
const styles = (theme): StyleRules => {
  return {
    schemaContainer: {
      margin: '5px 0',
    },
    stageName: { wordBreak: 'break-all' },
    schemaTableHeaderCell: {
      fontSize: '13px',
      borderBottom: 0,
    },
    headerRow: {
      display: 'flex',
      width: '100%',
    },
    fieldNameHeader: {
      display: 'inline',
      width: '45%',
      paddingRight: '10px',
    },
    aliasHeader: {
      width: '45%',
    },
    schemaSelectColumnHeader: {
      paddingLeft: 0,
      paddingRight: '10px',
      width: '80px',
    },
    tableHeaderSelectIcon: {
      marginLeft: '5px',
    },
    badgeDanger: {
      backgroundColor: theme.palette.red[200],
      color: 'white',
      fontSize: '9px',
      marginLeft: '5px',
      maxHeight: '20px',
    },
    // Table cell has grey border bottom by default, this causes issues when we
    // changeborder to red for rows (cell overrides row color). So, we remove
    // any border from table cells, add border to rows
    tableRow: {
      '& th': {
        border: 0,
      },
      borderBottom: `1px solid ${theme.palette.grey[400]}`,
    },
  };
};

interface ISchemaContainerProps extends WithStyles<typeof styles> {
  stage: IParsedInputSchema;
  onExpandClick: (stage: IParsedInputSchema) => void;
  onSchemaChange: (stage: IParsedInputSchema) => void;
  aliases: object;
  errorCount: number;
  disabled: boolean;
  errors: IErrorObj[];
}

interface ISchemaContainerState {
  selectFields: string;
  menuAnchor: HTMLButtonElement;
}

class SchemaContainer extends React.Component<ISchemaContainerProps, ISchemaContainerState> {
  public state = {
    selectFields: 'All',
    menuAnchor: null,
  };

  private tableSelectChange = (selectFields) => {
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

  private handleMenuClose = (value) => {
    this.setState({ menuAnchor: null }, () => {
      if (value) {
        this.tableSelectChange(value);
      }
    });
  };

  private handleMenuOpen = (event) => {
    this.setState({ menuAnchor: event.currentTarget });
  };

  private constructNewStage = (newSchema) => {
    return {
      ...this.props.stage,
      schema: newSchema,
    };
  };

  private toggleExpansionPanel = () => {
    this.props.onExpandClick(this.props.stage);
  };

  private updateField = (newField) => {
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

  private getValidationError(field) {
    let validationError = '';
    if (field.selected && this.props.errors) {
      // Element format is 'stageName.fieldName as alias'.
      const curElement = `${this.props.stage.name}.${field.name} as ${field.alias}`;
      const curError = this.props.errors.find((err: IErrorObj) => err.element === curElement);
      if (curError) {
        validationError = curError.msg;
      }
    }
    return validationError;
  }

  public render() {
    const { classes, errors } = this.props;
    return (
      <ExpansionPanel
        className={classes.schemaContainer}
        expanded={this.props.stage.expanded}
        onChange={this.toggleExpansionPanel}
      >
        <ExpansionPanelSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="h6" className={classes.stageName}>
            {this.props.stage.name}
          </Typography>
          <If condition={Boolean(this.props.errorCount)}>
            <span className={classnames('badge', classes.badgeDanger)}>
              {this.props.errorCount}
            </span>
          </If>
        </ExpansionPanelSummary>
        <ExpansionPanelDetails>
          <Table size="small">
            <TableHead>
              <TableRow className={classes.tableRow}>
                <TableCell>
                  <div className={classes.headerRow}>
                    <div
                      className={classnames(classes.schemaTableHeaderCell, classes.fieldNameHeader)}
                    >
                      <Typography variant="h6" display="inline">
                        {T.translate(`${I18N_PREFIX_TABLE}.stageNameHeader`)}
                      </Typography>
                    </div>
                    <div
                      className={classnames(
                        classes.schemaTableHeaderCell,
                        classes.schemaSelectColumnHeader
                      )}
                    >
                      <Button
                        disabled={this.props.disabled}
                        size="small"
                        variant="outlined"
                        onClick={this.handleMenuOpen}
                      >
                        <span>{T.translate(`${I18N_PREFIX_TABLE}.checkboxHeader`)}</span>
                        <IconSVG name="icon-caret-down" className={classes.tableHeaderSelectIcon} />
                      </Button>
                      <Menu
                        anchorEl={this.state.menuAnchor}
                        open={Boolean(this.state.menuAnchor)}
                        onClose={this.handleMenuClose}
                      >
                        <MenuItem onClick={() => this.handleMenuClose('All')}>
                          {T.translate(`${I18N_PREFIX_TABLE}.selectMenuItems.all`)}
                        </MenuItem>
                        <MenuItem onClick={() => this.handleMenuClose('None')}>
                          {T.translate(`${I18N_PREFIX_TABLE}.selectMenuItems.none`)}
                        </MenuItem>
                      </Menu>
                    </div>
                    <div className={classnames(classes.schemaTableHeaderCell, classes.aliasHeader)}>
                      <Typography variant="h6" display="inline">
                        {T.translate(`${I18N_PREFIX_TABLE}.aliasHeader`)}
                      </Typography>
                    </div>
                  </div>
                </TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {this.props.stage.schema.map((field, i) => {
                const aliasError = this.props.aliases[field.alias] > 1;
                return (
                  <FieldRow
                    key={`${i}-${field.name}`}
                    error={aliasError}
                    field={field}
                    onFieldChange={this.updateField}
                    disabled={this.props.disabled}
                    validationError={this.getValidationError(field)}
                  />
                );
              })}
            </TableBody>
          </Table>
        </ExpansionPanelDetails>
      </ExpansionPanel>
    );
  }
}

export default withStyles(styles)(SchemaContainer);

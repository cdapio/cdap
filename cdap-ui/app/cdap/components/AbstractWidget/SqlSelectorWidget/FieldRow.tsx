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
import TableRow from '@material-ui/core/TableRow';
import TableCell from '@material-ui/core/TableCell';
import Checkbox from '@material-ui/core/Checkbox';
import Typography from '@material-ui/core/Typography';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import { IFieldSchema } from 'components/AbstractWidget/SqlSelectorWidget';
import TextField from '@material-ui/core/TextField';

const styles = (theme): StyleRules => {
  return {
    // Table cell has grey border bottom by default, this causes issues when we
    // changeborder to red for rows (cell overrides row color). So, we remove
    // any border from table cells, add border to rows
    tableRow: {
      '& td': {
        border: 0,
      },
      borderBottom: `1px solid ${theme.palette.grey[400]}`,
    },
    errorRow: {
      // each errored cell would have red top border to combat row border from
      // previous row
      '& td': {
        borderTop: `1px solid ${theme.palette.red[200]}`,
      },
      border: `1px solid ${theme.palette.red[200]}`,
    },
    selectCell: {
      paddingLeft: 0,
      paddingRight: 0,
    },
  };
};
interface IFieldRowProps extends WithStyles<typeof styles> {
  onFieldChange: (newField: IFieldSchema) => void;
  field: IFieldSchema;
  error: boolean;
  disabled: boolean;
}

const FieldRow: React.FC<IFieldRowProps> = ({
  onFieldChange,
  field,
  error,
  classes,
  disabled,
}: IFieldRowProps) => {
  const aliasChange = (event) => {
    onFieldChange({ ...field, alias: event.target.value });
  };
  const selectedChange = () => {
    onFieldChange({ ...field, selected: !field.selected });
  };

  return (
    <TableRow
      className={classnames({
        [classes.tableRow]: true,
        [classes.errorRow]: error,
      })}
    >
      <TableCell>
        <Typography variant="body1">{field.name}</Typography>
      </TableCell>
      <TableCell className={classes.selectCell} align="left" padding="checkbox">
        <Checkbox
          disabled={disabled}
          checked={field.selected}
          value={field.selected}
          color="primary"
          onClick={selectedChange}
        />
      </TableCell>
      <TableCell>
        <TextField
          fullWidth
          margin="dense"
          variant="outlined"
          value={field.alias}
          onChange={aliasChange}
          disabled={!field.selected || disabled}
        />
      </TableCell>
    </TableRow>
  );
};

export default withStyles(styles)(FieldRow);

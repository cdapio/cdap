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

import TableCell from '@material-ui/core/TableCell';
import Checkbox from '@material-ui/core/Checkbox';
import Typography from '@material-ui/core/Typography';

import { IFieldSchema } from 'components/AbstractWidget/SqlSelectorWidget';

import Input from 'components/AbstractWidget/DefaultInput';

interface IFieldRowProps {
  onFieldChange: (arg0: any) => void;
  field: IFieldSchema;
}

function FieldRow({ onFieldChange, field }: IFieldRowProps) {
  const aliasChange = (alias) => {
    onFieldChange({ ...field, alias });
  };
  const selectedChange = () => {
    onFieldChange({ ...field, selected: !field.selected });
  };

  return (
    <React.Fragment>
      <TableCell>
        <Typography variant="body1">{field.name}</Typography>
      </TableCell>
      <TableCell align="center">
        <Checkbox
          checked={field.selected}
          value={field.selected}
          color="primary"
          onClick={() => {
            selectedChange();
          }}
        />
      </TableCell>
      <TableCell>
        <Input
          widgetProps={{ disabled: !field.selected }}
          value={field.alias}
          onChange={(e) => {
            aliasChange(e.target.value);
          }}
        />
      </TableCell>
    </React.Fragment>
  );
}

(FieldRow as any).propTypes = {
  onFieldChange: PropTypes.func,
  field: PropTypes.any,
  alias: PropTypes.string,
};

export default React.memo(FieldRow);

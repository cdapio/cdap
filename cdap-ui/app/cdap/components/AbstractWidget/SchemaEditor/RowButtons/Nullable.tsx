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
import CheckBox from '@material-ui/core/Checkbox';
import CheckBoxIcon from '@material-ui/icons/CheckBox';
import CheckBoxOutlineBlankIcon from '@material-ui/icons/CheckBoxOutlineBlank';

interface INullableBaseProps {
  nullable: boolean;
  onNullable: (value: boolean) => void;
}

const NullableBase = ({ nullable, onNullable: onChange }: INullableBaseProps) => {
  return (
    <CheckBox
      data-cy="schema-field-nullable-checkbox"
      checked={nullable}
      color="primary"
      checkedIcon={<CheckBoxIcon fontSize="small" />}
      icon={<CheckBoxOutlineBlankIcon fontSize="small" />}
      onChange={(e: React.ChangeEvent<HTMLInputElement>) => {
        onChange(e.target.checked);
      }}
      disabled={typeof onChange !== 'function'}
    />
  );
};

const Nullable = React.memo(NullableBase);
export { Nullable };

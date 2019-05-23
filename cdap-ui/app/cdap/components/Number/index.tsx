/*
 * Copyright © 2019 Cask Data, Inc.
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

import React, { useState } from 'react';
import ThemeWrapper from 'components/ThemeWrapper';
import PropTypes from 'prop-types';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import FormControl from '@material-ui/core/FormControl';
import Input from '@material-ui/core/Input';

const styles = (theme) => {
  return {
    root: {
      border: `1px solid ${theme.palette.grey['300']}`,
      borderRadius: 4,
      margin: '10px 0 10px 10px',
    },
  };
};

interface INumberProps extends WithStyles<typeof styles> {
  disabled?: boolean;
  isFieldRequired?: boolean;
}

function Number(props: INumberProps) {
  // not required and not disabled by default
  // default value for number input?
  const [value, setValue] = useState();

  return (
    <div>
      <FormControl
        onChange={(e) => {
          const target = event.target as HTMLInputElement;
          setValue(target.value);
        }}
      >
        <Input
          type="number"
          value={value}
          required={props.isFieldRequired}
          disabled={props.disabled}
        />
      </FormControl>
    </div>
  );
}

const NumberWrapper = withStyles(styles)(Number);

export default function StyledNumber(props) {
  return (
    <ThemeWrapper>
      <NumberWrapper {...props} />
    </ThemeWrapper>
  );
}

(StyledNumber as any).propTypes = {
  disabled: PropTypes.bool,
  isFieldRequired: PropTypes.bool
};

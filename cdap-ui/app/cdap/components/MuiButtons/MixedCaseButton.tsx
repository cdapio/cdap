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
import Button from '@material-ui/core/Button';
import IButtonProps from './IButtonProps'
import { withStyles } from '@material-ui/core/styles';

const StyledButton = withStyles({
  label: {
    textTransform: 'capitalize'
  }
})(Button);

const MixedCaseButton: React.FC<IButtonProps> = (props) => {
  return <StyledButton variant="contained" color="primary" {...props}>
    {props.children}
  </StyledButton>   
}

export default MixedCaseButton;
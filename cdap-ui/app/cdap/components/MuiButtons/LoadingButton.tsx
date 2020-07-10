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
import SvgIcon from '@material-ui/core/SvgIcon';
import IButtonProps from './IButtonProps'
import LoadingSVG from 'components/LoadingSVG';
import BackupIcon from '@material-ui/icons/Backup';

interface ILoadingButtonProps extends IButtonProps {
  loading: boolean
}

// TODO What icon should be shown if loading=false? If `undefined` is sent, the button changes width
const LoadingButton: React.FC<ILoadingButtonProps> = (props) => {
  const {
    disabled,
    loading,
    ...others
  } = props;

  return <Button 
    variant="outlined"
    disabled={ disabled || loading }
    startIcon={ loading ? <SvgIcon viewBox="0 0 24 30"><LoadingSVG /></SvgIcon> : <BackupIcon /> }
    {...others}>
    { props.children }
  </Button>
}

export default LoadingButton;
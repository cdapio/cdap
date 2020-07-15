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

import Dialog from '@material-ui/core/Dialog';
import MuiDialogTitle from '@material-ui/core/DialogTitle';
import MuiDialogContent from '@material-ui/core/DialogContent';
import MuiDialogActions from '@material-ui/core/DialogActions';

import { TextButton } from 'components/MuiButtons';

interface IConfirmationDialogProps {
  confirmationText: string,
  onAccept: () => void,
  onCancel: () => void,
  open: boolean,
  title: string,
}

const ConfirmationDialog: React.FC<IConfirmationDialogProps> = ({ 
  children, 
  confirmationText,
  onAccept,
  onCancel, 
  open, 
  title 
}) => {

  return <Dialog open={open} onClose={ onCancel }>
    <MuiDialogTitle>
      { title }
    </MuiDialogTitle>
    <MuiDialogContent>
      { children }
    </MuiDialogContent>
    <MuiDialogActions>
      <TextButton onClick={ onCancel }>Close</TextButton>
      <TextButton onClick={ onAccept }>{ confirmationText }</TextButton>
    </MuiDialogActions>
  </Dialog>
}

export default ConfirmationDialog;
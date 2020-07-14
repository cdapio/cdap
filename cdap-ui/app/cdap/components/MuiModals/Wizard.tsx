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
import DialogTitle from './DialogTitle';
import DialogContent from './DialogContent';
import MuiDialogActions from '@material-ui/core/DialogActions';
import Drawer from '@material-ui/core/Drawer';
import List from'@material-ui/core/List';
import ListItem from'@material-ui/core/ListItem';
import ListItemText from'@material-ui/core/ListItemText';

import { PrimaryButton, SecondaryButton } from 'components/MuiButtons';
import { Typography } from '@material-ui/core';

interface IModalProps {
  onClose: () => any,
  open: boolean,
}

const Wizard: React.FC<IModalProps> = ({ onClose, open }) => {

  return <Dialog open={open} onClose={ onClose }>
    <DialogTitle onClose={ onClose }>
      Default Title
    </DialogTitle>
    <DialogContent>
      <main>
        <Typography gutterBottom>
          Cras mattis consectetur purus sit amet fermentum. Cras justo odio, dapibus ac facilisis
          in, egestas eget quam. Morbi leo risus, porta ac consectetur ac, vestibulum at eros.
        </Typography>
        <Typography gutterBottom>
          Praesent commodo cursus magna, vel scelerisque nisl consectetur et. Vivamus sagittis
          lacus vel augue laoreet rutrum faucibus dolor auctor.
        </Typography>
        <Typography gutterBottom>
          Aenean lacinia bibendum nulla sed consectetur. Praesent commodo cursus magna, vel
          scelerisque nisl consectetur et. Donec sed odio dui. Donec ullamcorper nulla non metus
          auctor fringilla.
        </Typography>
      </main>
    </DialogContent>
    <MuiDialogActions>
      <SecondaryButton>Cancel</SecondaryButton>
      <SecondaryButton>Previous</SecondaryButton>
      <SecondaryButton>Next</SecondaryButton>
      <PrimaryButton>Finish</PrimaryButton>
    </MuiDialogActions>
  </Dialog>
}

export default Wizard;
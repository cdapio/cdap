/*
 * Copyright © 2018 Cask Data, Inc.
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

import { storiesOf } from '@storybook/react';
import { action } from '@storybook/addon-actions';
import { withInfo } from '@storybook/addon-info';
import { withKnobs, boolean, text } from '@storybook/addon-knobs';
import Typography from '@material-ui/core/Typography';
import DialogContentText from '@material-ui/core/DialogContentText';
import TextField from '@material-ui/core/TextField';
import Alert from '@material-ui/lab/Alert';
import FormControl from '@material-ui/core/FormControl';
import FormLabel from '@material-ui/core/FormLabel';
import FormControlLabel from '@material-ui/core/FormControlLabel';
import RadioGroup from '@material-ui/core/RadioGroup';
import Radio from '@material-ui/core/Radio';

import If from 'components/If';

import { ConfirmationDialog, FormDialog, InformationDialog, StyledModal, Wizard } from './index';

storiesOf('Material Modals and Dialogs', module)
  .addDecorator(withKnobs)
  .add(
    'Styled Modal',
    withInfo({
      text: 'Basic modal (current styling)'
    })(() => (
      <React.Fragment>
        <Typography variant="h2">Material Dialog modal</Typography>
        <StyledModal onClose={ action('closed') } open={ boolean('Open', true) } />
      </React.Fragment>
    ))
  )
  .add(
    'Wizard',
    withInfo({
      text: 'Wizard UI concept'
    })(() => (
      <React.Fragment>
        <Typography variant="h2">Material Dialog wizard concept</Typography>
        <Wizard onClose={ action('closed') } open={ boolean('Open', true) } />
      </React.Fragment>
    ))
  )
  .add(
    'InformationDialog',
    withInfo({
      text: 'Informational dialog (Carbon stlying)'
    })(() => (
      <React.Fragment>
        <Typography variant="h2">Informational Dialog</Typography>
        <InformationDialog 
          onClose={ action('closed') } 
          open={ boolean('Open', true) }
          title={ text('Title', 'An important message') }
        >
          <Typography>
            { text('Content', 'This is the content of the dialog.') }
          </Typography>
        </InformationDialog>
      </React.Fragment>
    ))
  )
  .add(
    'ConfirmationDialog',
    withInfo({
      text: 'Confirmation dialog (Carbon stlying)'
    })(() => (
      <React.Fragment>
        <Typography variant="h2">Confirmation Dialog</Typography>
        <ConfirmationDialog 
          confirmationText={ text('Accept text', 'Continue') }
          onAccept={ action('accept') } 
          onCancel={ action('cancel') } 
          open={ boolean('Open', true) }
          title={ text('Title', 'Confirm operation') }
        >
          <Typography>
            { text('Content', 'Are you sure you want to `rm -rf /`? This action cannot be undone.') }
          </Typography>
        </ConfirmationDialog>
      </React.Fragment>
    ))
  )
  .add(
    'FormDialog',
    withInfo({
      text: 'Form dialog (Carbon stlying)'
    })(() => {
      const error = boolean('Error', false);
      return <React.Fragment>
        <Typography variant="h2">Form Input Dialog</Typography>
        <FormDialog 
          submitText={ text('Accept text', 'Save') }
          onSubmit={ action('accept') } 
          onCancel={ action('cancel') } 
          canSubmit={ !error }
          open={ boolean('Open', true) }
          title={ text('Title', 'Import data from Cloud Storage') }
        >
          <DialogContentText>
            { text('Content', 'Choose a Cloud Storage file to import into your Cloud SQL instance. Needs more work on vertical spacing.') }
          </DialogContentText>
          <If condition={ error === true }>
            <Alert severity="error">
              { text('Error message', 'The specified resource was not found') }
            </Alert>
          </If>
          <TextField
            autoFocus
            error={ error }
            fullWidth
            helperText={ error ? 'Resource not found' : undefined}
            id="location"
            label="Cloud Storage file"
            margin="dense"
            placeholder="bucket/folder/title"
          />
          <FormControl component="fieldset" margin="dense">
            <FormLabel component="legend">Format of Import</FormLabel>
            <RadioGroup aria-label="format" name="format" value="SQL">
              <FormControlLabel value="SQL" control={<Radio />} label="SQL" />
              <FormControlLabel value="CSV" control={<Radio />} label="CSV" />
            </RadioGroup>
          </FormControl>
        </FormDialog>
      </React.Fragment>
    })
  )
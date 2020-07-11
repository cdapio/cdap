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
import { SecondaryButton } from 'components/MuiButtons';
import Typography from '@material-ui/core/Typography';
import If from 'components/If';

export default class  NotificationPermission extends React.Component {

  render() {
    const handlePermissionRequest = (event: React.ChangeEvent<HTMLInputElement>) => {
      // TODO This won't run Safari
      Notification.requestPermission().then(() => this.forceUpdate());
    };
  
    return <React.Fragment>
      <SecondaryButton disabled={ !Notification || Notification.permission !== 'default' } onClick={ handlePermissionRequest }>Change notification permission</SecondaryButton>
      <If condition={ Notification.permission === 'granted' }>
        <Typography variant="body2">
          Notifications are enabled
        </Typography>
      </If>
      <If condition={ Notification.permission === 'denied' }>
        <Typography variant="body2">
          Notifications are disabled
        </Typography>
      </If>
    </React.Fragment>
  }
}
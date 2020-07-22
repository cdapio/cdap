/*
 * Copyright © 2017-2020 Cask Data, Inc.
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

import { WithStyles, withStyles } from '@material-ui/core';

import HttpExecutorStore from 'components/HttpExecutor/store/HttpExecutorStore';
import HttpResponse from 'components/HttpExecutor/HttpResponse';
import InputPath from 'components/HttpExecutor/InputPath';
import MethodSelector from 'components/HttpExecutor/MethodSelector';
import { Provider } from 'react-redux';
import React from 'react';
import RequestHistoryTab from 'components/HttpExecutor/RequestHistoryTab';
import RequestMetadata from 'components/HttpExecutor/RequestMetadata';
import SaveCalls from 'components/HttpExecutor/SaveCalls';
import SendButton from 'components/HttpExecutor/SendButton';
import StatusCode from 'components/HttpExecutor/StatusCode';
import { StyleRules } from '@material-ui/core/styles';
import T from 'i18n-react';

const PREFIX = 'features.HttpExecutor';

require('./HttpExecutor.scss');

export const LEFT_PANEL_WIDTH = 500;

export enum RequestMethod {
  GET = 'GET',
  POST = 'POST',
  PUT = 'PUT',
  DELETE = 'DELETE',
}

const styles = (theme): StyleRules => {
  return {
    root: {
      borderRight: `1px solid ${theme.palette.grey[300]}`,
      height: '100%',
    },
    content: {
      height: 'calc(100% - 50px)',
      display: 'grid',
      gridTemplateColumns: `${LEFT_PANEL_WIDTH}px 1fr`,

      '& > div': {
        overflowY: 'auto',
      },
    },
    introPageRow: {
      display: 'grid',
      width: '100%',
      gridTemplateColumns: 'repeat(7, 1fr)',
      paddingTop: `${theme.Spacing(2)}px`,
    },
    pageTitle: {
      fontSize: '20px',
      paddingLeft: `${theme.Spacing(3)}px`,
      gridColumnStart: '1',
      gridColumnEnd: '2',
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'center',
    },
    saveCalls: {
      gridColumnStart: '7',
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'center',
    },
  };
};

const HttpExecutorView: React.FC<WithStyles<typeof styles>> = ({ classes }) => {
  return (
    <Provider store={HttpExecutorStore}>
      <div className={classes.content}>
        <RequestHistoryTab />
        <div className="http-executor">
          <div className={classes.introPageRow}>
            <div className={classes.pageTitle}>Http calls executor</div>
            <div className={classes.saveCalls}>
              <SaveCalls />
            </div>
          </div>

          <div className="request-section">
            <MethodSelector />
            <InputPath />

            <SendButton />
          </div>
          <RequestMetadata />
          <div className="response-section">
            <div className="response-header">
              <span className="title">{T.translate(`${PREFIX}.responseTitle`)}</span>
              <span className="float-right">
                <StatusCode />
              </span>
            </div>
            <HttpResponse />
          </div>
        </div>
      </div>
    </Provider>
  );
};

const HttpExecutor = withStyles(styles)(HttpExecutorView);
export default HttpExecutor;

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

import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';

const styles = (): StyleRules => {
  return {
    JSONLiveCode: {
      overflow: 'auto',
      height: '100%',
    },
    output: {
      whiteSpace: 'pre-wrap',
      // Full height excluding header, footer, and top panel
      // 100vh - header - footer - top panel
      // 100vh - 48px - 53px - 40px
      height: 'calc(100vh - 141px)',
      overflow: 'scroll',
      padding: '14px',
    },
  };
};

interface ILiveJSONProps extends WithStyles<typeof styles> {
  JSONOutput: any;
}

const LiveJSONView: React.FC<ILiveJSONProps> = ({ classes, JSONOutput }) => {
  return (
    <div className={classes.JSONLiveCode}>
      <pre className={classes.output} data-cy="live-json">
        {JSON.stringify(JSONOutput, undefined, 2)}
      </pre>
    </div>
  );
};

const LiveJSON = withStyles(styles)(LiveJSONView);
export default LiveJSON;

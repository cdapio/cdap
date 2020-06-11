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

import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';
import * as React from 'react';

const styles = (theme): StyleRules => {
  return {
    JSONLiveCode: {
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
      <pre>{JSON.stringify(JSONOutput, undefined, 2)}</pre>
    </div>
  );
};

const LiveJSON = withStyles(styles)(LiveJSONView);
export default LiveJSON;

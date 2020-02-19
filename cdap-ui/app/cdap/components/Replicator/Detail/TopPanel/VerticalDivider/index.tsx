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
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';

const styles = (theme): StyleRules => {
  return {
    root: {
      borderRight: `2px solid ${theme.palette.grey[300]}`,
      height: '50px',
      marginLeft: '15px',
      marginRight: '15px',
    },
  };
};

const VerticalDividerView: React.FC<WithStyles<typeof styles>> = ({ classes }) => {
  return <div className={classes.root} />;
};

const VerticalDivider = withStyles(styles)(VerticalDividerView);
export default VerticalDivider;

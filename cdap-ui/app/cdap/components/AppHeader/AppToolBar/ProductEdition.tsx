/*
 * Copyright © 2019 Cask Data, Inc.
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
import { Theme } from 'services/ThemeHelper';
import Typography from '@material-ui/core/Typography';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';

const styles = (theme) => {
  return {
    productEdition: {
      'max-width': '100px',
      padding: '0 15px',
      'border-left': `2px solid ${theme.palette.grey[400]}`,
    },
    caption: {
      color: theme.palette.grey[700],
      'line-height': 1.3,
      display: 'block',
    },
  };
};

const ProductEdition: React.SFC<WithStyles<typeof styles>> = ({ classes }) => {
  if (!Theme.productEdition) {
    return null;
  }

  // Splitting the names to different <span> so that the max-width can take effect
  const splitEditionName = Theme.productEdition.split(' ');

  return (
    <div className={classes.productEdition}>
      {splitEditionName.map((line, i) => {
        return (
          <Typography variant="caption" className={classes.caption} key={i}>
            {line}
          </Typography>
        );
      })}
    </div>
  );
};

export default withStyles(styles)(ProductEdition);

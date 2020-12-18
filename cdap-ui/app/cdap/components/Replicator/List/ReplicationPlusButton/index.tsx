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
import PlusButton from 'components/PlusButton';
import { getCurrentNamespace } from 'services/NamespaceStore';

const styles = (): StyleRules => {
  return {
    root: {
      position: 'absolute',
      top: '20px',
      right: '25px',
      zIndex: 1,
    },
  };
};

const ReplicationPlusButtonView: React.FC<WithStyles<typeof styles>> = ({ classes }) => {
  const contextItems = [
    {
      label: 'Create a replication pipeline',
      to: `/ns/${getCurrentNamespace()}/replication/create`,
    },
  ];

  return (
    <div className={classes.root}>
      <PlusButton mode={PlusButton.MODE.resourcecenter} contextItems={contextItems} />
    </div>
  );
};

const ReplicationPlusButton = withStyles(styles)(ReplicationPlusButtonView);
export default ReplicationPlusButton;

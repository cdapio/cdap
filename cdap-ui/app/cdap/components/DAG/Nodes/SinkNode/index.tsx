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

import withStyles from '@material-ui/core/styles/withStyles';
import { IAbstractNodeProps, AbstractNode } from 'components/DAG/Nodes/AbstractNode';
import { genericNodeStyles } from 'components/DAG/Nodes/utilities';

const styles = genericNodeStyles({
  border: `1px solid #8367df`,
  '&.drag-hover': {
    backgroundColor: 'rgba(131, 103, 223, 0.1)',
  },
});
interface ISinkNodeProps extends IAbstractNodeProps<typeof styles> {}
class SinkNodeComponent extends AbstractNode<ISinkNodeProps> {
  public type = 'sink';

  public checkForValidIncomingConnection = (connObj) => {
    if (connObj.connection.target.getAttribute('data-node-type') !== 'sink') {
      return true;
    }
    return (
      ['alertpublisher', 'sink'].indexOf(
        connObj.connection.source.getAttribute('data-node-type')
      ) === -1
    );
  };
}

const SinkNode = withStyles(styles)(SinkNodeComponent);
export { SinkNode };

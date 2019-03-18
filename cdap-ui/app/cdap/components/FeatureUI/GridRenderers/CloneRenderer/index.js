/*
 * Copyright © 2017 Cask Data, Inc.
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

import React from 'react';
import PropTypes from 'prop-types';

class CloneRenderer extends React.Component {
  constructor(props) {
    super(props);
  }
  invokeParentMethod(item) {
    if (this.props.context && this.props.context.componentParent && this.props.context.componentParent.onClone) {
      this.props.context.componentParent.onClone(item);
    }
  }
  render() {
    return <span className="fa fa-clone"
      onClick={this.invokeParentMethod.bind(this, this.props.data)}>
    </span>;

  }
}
export default CloneRenderer;
CloneRenderer.propTypes = {
  context: PropTypes.object,
  data: PropTypes.any
};

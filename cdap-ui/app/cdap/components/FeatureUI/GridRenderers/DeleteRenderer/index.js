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

class DeleteRenderer extends React.Component {
  constructor(props) {
    super(props);
  }
  invokeParentMethod(item) {
    if (this.props.context && this.props.context.componentParent && this.props.context.componentParent.onDelete) {
      this.props.context.componentParent.onDelete(item);
    }
  }
  render() {
    return <span className="fa fa-trash text-danger"
      onClick={this.invokeParentMethod.bind(this, this.props.data)}>
    </span>;

  }
}
export default DeleteRenderer;
DeleteRenderer.propTypes = {
  context: PropTypes.object,
  data: PropTypes.any
};

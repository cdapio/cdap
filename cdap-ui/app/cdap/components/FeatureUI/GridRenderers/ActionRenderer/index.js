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
import { PipeLineStatusConfig } from '../../config';
import { isNil } from 'lodash';
import { EDIT, CLONE, DELETE } from '../../config';


class ActionRenderer extends React.Component {
  constructor(props) {
    super(props);
  }


  invokeParentMethod(item, type) {
    if (this.isValidAction(item, type) && this.props.context && this.props.context.componentParent && this.props.context.componentParent.onAction) {
      this.props.context.componentParent.onAction(item, type);
    }
  }

  isValidAction(item, type) {
    let result = true;
    let status = isNil(item) ? undefined : item.status;
    if (!isNil(status)) {
      PipeLineStatusConfig.forEach(element => {
        if (element.name === status.toUpperCase()) {
          result = element[type];
        }
      });
    }
    // if the pipeline type is feature selection then edit and clone option will be disable.
    if (type != DELETE) {
      let pipeLineType = isNil(item) ? undefined : item.pipelineType;
      if (!isNil(pipeLineType)) {
        if (pipeLineType === "Selected Feature Pipeline") {
          result = false;
        }
      }
    }
    return result;
  }


  render() {

    let type = this.props.colDef.cellRendererParams.action;
    let actionClass = "fa";
    if (type == CLONE) {
      actionClass += " fa-clone";
    } else if (type == EDIT) {
      actionClass += " fa-edit";
    } else if (type == DELETE) {
      actionClass += " fa-trash text-danger";
    }

    let validAction = this.isValidAction(this.props.data, type);
    return validAction ? <span className={actionClass}
      onClick={this.invokeParentMethod.bind(this, this.props.data, type)}>
    </span> : null;

  }
}
export default ActionRenderer;
ActionRenderer.propTypes = {
  context: PropTypes.object,
  data: PropTypes.any,
  colDef:PropTypes.object
};

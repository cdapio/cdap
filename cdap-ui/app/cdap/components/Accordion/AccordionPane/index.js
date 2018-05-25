/*
 * Copyright © 2018 Cask Data, Inc.
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

import React, {PureComponent} from 'react';
import PropTypes from 'prop-types';
import classnames from 'classnames';

require('./AccordionPane.scss');
export default class AccordionPane extends PureComponent {
  static propTypes = {
    id: PropTypes.string.isRequired,
    onTabPaneClick: PropTypes.func,
    activePane: PropTypes.string,
    size: PropTypes.oneOf(["small", "medium", "large"]),
    children: PropTypes.node
  };

  render() {
    let {
      onTabPaneClick,
      activePane,
      size,
      id
    } = this.props;
    return (
      <div className={classnames("accordion-pane", {
        active: this.props.activePane === this.props.id
      })}>
        {
          React.Children.map(this.props.children, child => {
            if (
              child.type.displayName === 'AccordionTitle' ||
              child.type.name === 'AccordionTitle' ||
              this.props.activePane === this.props.id
            ) {
              return React.cloneElement(child, {
                ...child.props,
                onTabPaneClick,
                activePane,
                size,
                id,
                key: this.props.id
              });
            }
            return null;
          })
        }
      </div>
    );
  }
}

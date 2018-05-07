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

import PropTypes from 'prop-types';
import React, {PureComponent} from 'react';
import AccordionPane from 'components/Accordion/AccordionPane';
import AccordionTitle from 'components/Accordion/AccordionTitle';
import AccordionContent from 'components/Accordion/AccordionContent';

import uuidV4 from 'uuid/v4';
export {
  AccordionContent,
  AccordionTitle,
  AccordionPane
};

export default class Accordion extends PureComponent {

  static propTypes = {
    size: PropTypes.oneOf(["small", "medium", "large"]),
    active: PropTypes.string.isRequired,
    children: PropTypes.node
  };

  state = {
    activePane: this.props.active
  };

  onTabPaneClick = (tabPaneId) => {
    if (this.state.activePane === tabPaneId) {
      this.setState({
        activePane: null
      });
      return;
    }
    this.setState({
      activePane: tabPaneId
    });
  };

  render() {
    return (
      <div className="accordion">
        {
          React.Children.map(this.props.children, child => {
            return React.cloneElement(child, {
              ...child.props,
              activePane: this.state.activePane,
              onTabPaneClick: this.onTabPaneClick,
              size: this.props.size,
              key: uuidV4()
            });
          })
        }
      </div>
    );
  }
}

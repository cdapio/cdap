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
import React, {Component} from 'react';
import classnames from 'classnames';
import Popover from 'components/Popover';

require('./CollapsibleWrapper.scss');

export default class CollapsibleWrapper extends Component {
  static propTypes = {
    content: PropTypes.string,
    popoverContent: PropTypes.element,
    alwaysShowViewLink: PropTypes.bool
  };

  static defaultProps = {
    alwaysShowViewLink: false
  };

  state = {
    showViewLink: false
  };

  componentWillMount() {
    window.addEventListener('resize', this.setViewLink);
  }

  componentWillUnmount() {
    window.removeEventListener('resize', this.setViewLink);
  }

  setViewLink = () => {
    if (this.contentRef) {
      this.setState({
        showViewLink: this.contentRef.offsetWidth >= this.contentRef.parentElement.offsetWidth
      });
    }
  };

  componentDidMount() {
    this.setViewLink();
  }

  render() {
    return (
      <div
        ref={(ref) => this.contentRef = ref}
        className="collapsable-wrapper"
      >
        <span
          className={classnames({
            "with-view-link": this.state.showViewLink
          })}
        >
          {this.props.content}
        </span>
        {
          this.props.alwaysShowViewLink || this.state.showViewLink ?
          (
            <Popover
              target={() => <span className="view-wrapper"> View </span>}
              className="view-popover-wrapper"
              placement="right"
              bubbleEvent={false}
              enableInteractionInPopover={true}
            >
              {this.props.popoverContent()}
            </Popover>
          ): null
        }
      </div>
    );
  }
}

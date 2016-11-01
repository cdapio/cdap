/*
 * Copyright © 2016 Cask Data, Inc.
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

import React, {Component, PropTypes} from 'react';
import classNames from 'classnames';
import Mousetrap from 'mousetrap';
require('./Pagination.less');

export default class Pagination extends Component {

  constructor(props){
    super(props);
    this.state = {
      numResults : 0,
      leftPressed: false,
      rightPressed: false
    };
    this.goToNext = this.goToNext.bind(this);
    this.goToPrev = this.goToPrev.bind(this);
  }

  componentWillMount(){
    Mousetrap.bind('right', this.goToNext);
    Mousetrap.bind('left', this.goToPrev);
  }

  goToPrev() {
    //Highlight the side that is pressed
    this.setState({
      leftPressed: true
    });
    setTimeout(() => {
      this.setState({
        leftPressed: false
      });
    }, 250);

    this.props.setDirection('prev');
    this.props.setCurrentPage(this.props.currentPage-1);
  }

  goToNext(){
    //Highlight the side that is pressed
    this.setState({
      rightPressed: true
    });
    setTimeout(() => {
      this.setState({
        rightPressed: false
      });
    }, 250);

    this.props.setDirection('next');
    this.props.setCurrentPage(this.props.currentPage+1);
  }

  render() {
    let pageChangeRightClass = classNames('change-page-panel', 'change-page-panel-right', {'pressed' : this.state.rightPressed});
    let pageChangeLeftClass = classNames('change-page-panel', 'change-page-panel-left', {'pressed' : this.state.leftPressed});

    return (
      <div className="pagination-container">
        <div onClick={this.goToPrev}
          className={pageChangeLeftClass}
        >
          <div className="page-change-arrow-container">
            <span className="page-change-arrow fa fa-chevron-left fa-2x"></span>
          </div>
        </div>
        <div id="pagination-content">
          {this.props.children()}
        </div>
        <div onClick={this.goToNext}
          className={pageChangeRightClass}
        >
          <div className="page-change-arrow-container">
            <span className="page-change-arrow fa fa-chevron-right fa-2x"></span>
          </div>
        </div>
      </div>
    );
  }
}

Pagination.propTypes = {
  currentPage: PropTypes.number,
  children: PropTypes.func,
  setCurrentPage: PropTypes.func,
  // numPerPage: PropTypes.number,
  setDirection: PropTypes.func
};

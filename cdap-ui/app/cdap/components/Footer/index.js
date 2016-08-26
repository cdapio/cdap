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

import React, {Component} from 'react';

require('./Footer.less');

export default class Footer extends Component {
  constructor(props) {
    super(props);
    var {copyrightYear, version} = props;
    this.copyrightYear = copyrightYear || new Date().getFullYear();
    this.version = version || '--unknown--';
    this.props = props;
  }
  render() {
    return (
      <footer>
        <div className="container">
          <div className="row text-muted">
            <div className="col-sm-6 text-uppercase">
              <p>
                <span className="cask-logo"></span>
                <span>Copyright &copy; {this.copyrightYear} Cask Data, Inc.</span>
              </p>
            </div>
            <div className="col-sm-6">
              <ul className="list-inline">
                <li>
                  {this.version}
                </li>
                <li>
                  <a href="http://cask.co/terms" target="_blank">Terms</a>
                </li>
                <li>
                  <a href="http://cask.co/privacy" target="_blank">Privacy</a>
                </li>
                <li>
                  <a href="http://cask.co/contact" target="_blank">Contact</a>
                </li>
              </ul>
            </div>
          </div>
        </div>
      </footer>
    );
  }
}
Footer.propTypes = {
  version: React.PropTypes.string,
  copyrightYear: React.PropTypes.string
};

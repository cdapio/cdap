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

import Youtube from 'react-youtube';
import React, {Component} from 'react';

export default class CaskVideo extends Component {

  constructor(props){
    super(props);
  }

  render(){
    const opts = {
      height: '240',
      width: '560',
      playerVars: {
        autoplay: 1
      }
    };

    return (
      <Youtube
        videoId="MGKt2zheu1w"
        opts={opts}
      />
    );
  }

}

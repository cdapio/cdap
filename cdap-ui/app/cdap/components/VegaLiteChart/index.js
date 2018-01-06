/*
 * Copyright © 2017-2018 Cask Data, Inc.
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
import * as vl from 'vega-lite';
import * as vega from 'vega';
import * as vegaTooltip from 'vega-tooltip';
import shortid from 'shortid';
import LoadingSVG from 'components/LoadingSVG';
import debounce from 'lodash/debounce';
require('./VegaLiteChart.scss');
export default class VegaLiteChart extends Component {
  propTypes = {
    spec: PropTypes.object.isRequired,
    data: PropTypes.object,
    className: PropTypes.string,
    widthOffset: PropTypes.number,
    heightOffset: PropTypes.number,
    width: PropTypes.oneOfType([PropTypes.number, PropTypes.func]),
    tooltipOptions: PropTypes.object
  };
  state = {
    data: this.props.data || [],
    isLoading: true,
    id: `chart-${shortid.generate()}`
  };
  componentDidMount() {
    this.renderChart();
    document.body.onresize = debounce(this.renderChart, 1);
  }
  componentWillUnmount() {
    document.body.onresize = null;
  }
  componentWillReceiveProps(nextProps) {
    this.setState({data: nextProps.data || []}, this.renderChart.bind(this, true));
  }

  renderChart = (isResize) => {
    if (this.mountTimeout) {
      clearTimeout(this.mountTimeout);
    }
    if (!isResize) {
      this.setState({
        isLoading: true
      });
    }
    this.mountTimeout = window.setTimeout(() => {
      this.updateSpec();
      this.runView();
      this.setState({
        isLoading: false
      });
    });
  };

  updateSpec = () => {
    try {
      const el = document.getElementById(this.state.id);
      const dimension = el.getBoundingClientRect();
      const vlSpec = {
        ...this.props.spec,
        width: dimension.width - (this.props.widthOffset || 0),
        height: dimension.height - (this.props.heightOffset || 0),
        data: {
          name: this.state.id
        }
      };
      if (this.props.width) {
        if (typeof this.props.width === 'function') {
          vlSpec.width = this.props.width(dimension, this.state.data);
        } else {
          vlSpec.width = this.props.width;
        }
      }
      const spec = vl.compile(vlSpec).spec;
      const runtime = vega.parse(spec);
      this.view = new vega.View(runtime)
        .logLevel(vega.Warn)
        .initialize(el)
        .renderer('svg')
        .hover();
        if (this.props.tooltipOptions && Object.keys(this.props.tooltipOptions).length) {
          vegaTooltip.vega(this.view, this.props.tooltipOptions);
        } else {
          vegaTooltip.vega(this.view);
        }
      this.bindData();
    } catch (err) {
      console.log('ERROR: Failed to compile vega spec ', err);
    }
  };

  bindData = () => {
    const {data} = this.props;
    if (data) {
      this.view.change(this.state.id,
        vega.changeset()
            .remove(() => true) // remove previous data
            .insert(data)
      );
    }
  };

  runView = () => {
    try {
      this.view.run();
    } catch (err) {
      console.log('ERROR: Rendering view');
    }
  };

  render() {
    return (
      <div className={`${this.props.className} vega-lite-chart`}>
        {this.state.isLoading ? <LoadingSVG /> : null }
        <div id={this.state.id}></div>
      </div>
    );
  }
}

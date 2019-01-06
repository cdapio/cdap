/* eslint react/prop-types: 0 */
import React from 'react';
import NameValueList from '../NameValueList';
import cloneDeep from 'lodash/cloneDeep';

require('./Configurator.scss');

class Configurator extends React.Component {
  configPropList;
  constructor(props) {
    super(props);
    this.configPropList = [];
  }

  componentDidMount() {
    this.configPropList = cloneDeep(this.props.configurationList);
  }

  addConfiguration(nameValue) {
    this.configPropList.push(nameValue);
    this.props.updateConfigurationList(this.configPropList);
  }

  updateConfiguration(index, nameValue) {
    this.configPropList[index] = nameValue;
    this.props.updateConfigurationList(this.configPropList);
  }

  render() {
    return (
      <div className = 'configuration-step-container'>
        <NameValueList dataProvider = {this.props.configurationList}
         updateNameValue = {this.updateConfiguration.bind(this)}
         addNameValue = {this.addConfiguration.bind(this)}/>
      </div>
    );
  }
}
export default Configurator;


import React from 'react';
import { Input } from 'reactstrap';
require('./DetailProvider.scss');

class DetailProvider extends React.Component {
  name;
  constructor(props) {
    super(props);
  }

  onNameUpdated() {
    this.name =  event.target.value;
    this.props.updateFeatureName(this.name);
  }

  render() {
    return (
      <div className = "detail-step-container">
        <div className='field-row'>
            <div className='name'>Name</div>
            <div className='colon'>:</div>
            <Input className='value' type="text" name="value" placeholder='value'
              defaultValue = {this.props.featureName} onChange={this.onNameUpdated.bind(this)}/>
        </div>
      </div>
    );
  }
}

export default DetailProvider;
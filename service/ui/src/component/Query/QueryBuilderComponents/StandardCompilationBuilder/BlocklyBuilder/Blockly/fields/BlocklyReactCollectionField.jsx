/**
 * This shows you how to create a custom Blockly field that renders a react
 * component inside a drop down div when shown
 * @author Brandon Moore
 */
import React from 'react';
import ReactDOM from 'react-dom';
import FieldRenderComponent from './CollectionFieldRendererComponent';

import * as Blockly from 'blockly/core';
import {Provider} from 'react-redux';
import store from '../../../../../../../redux/store';

class BlocklyReactField extends Blockly.Field {

    static fromJson(options) {
        return new BlocklyReactField(options['text']);
    }

    onSelected (event) {
        this.setValue(event.name);
    }

    getText_() {
        return this.value_;
    }


    showEditor_() {
        this.div_ = Blockly.DropDownDiv.getContentDiv();
        ReactDOM.render(
            //added provider to allow the blockly custom component to access the redux store
            <Provider store={store}>
                {this.render()}
            </Provider>,
            this.div_);

        let border = this.sourceBlock_.getColourBorder();
        border = border.colourBorder || border.colourLight;
        Blockly.DropDownDiv.setColour(this.sourceBlock_.getColour(), border);

        Blockly.DropDownDiv.showPositionedByField(
            this, this.dropdownDispose_.bind(this));
    }

    dropdownDispose_() {
        ReactDOM.unmountComponentAtNode(this.div_);
    }

    render() {
        return <FieldRenderComponent onSelect={(event) => this.onSelected(event)}/>
    }
}


Blockly.fieldRegistry.register('collection_field_component', BlocklyReactField);

export default BlocklyReactField;
/**
 * This shows you how to create a custom Blockly field that renders a react
 * component inside a drop down div when shown
 * @author Brandon Moore
 */
import React from 'react';
import ReactDOM from 'react-dom';
import FieldRenderComponent from './FieldRenderComponent'

import * as Blockly from 'blockly/core';

class BlocklyReactField extends Blockly.Field {

    static fromJson(options) {
        return new BlocklyReactField(options['text']);
    }

    showEditor_() {
        this.div_ = Blockly.DropDownDiv.getContentDiv();
        ReactDOM.render(this.render(),
            this.div_);

        var border = this.sourceBlock_.getColourBorder();
        border = border.colourBorder || border.colourLight;
        Blockly.DropDownDiv.setColour(this.sourceBlock_.getColour(), border);

        Blockly.DropDownDiv.showPositionedByField(
            this, this.dropdownDispose_.bind(this));
    }

    dropdownDispose_() {
        ReactDOM.unmountComponentAtNode(this.div_);
    }

    render() {
        return <FieldRenderComponent />
    }
}


Blockly.fieldRegistry.register('field_react_component', BlocklyReactField);

export default BlocklyReactField;
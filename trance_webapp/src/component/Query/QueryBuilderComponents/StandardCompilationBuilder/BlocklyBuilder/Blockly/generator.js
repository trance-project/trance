/**
 * @fileoverview Define generation methods for custom blocks.
 * @author (Brandon Moore)
 */

// More on generating code:
// https://developers.google.com/blockly/guides/create-custom-blocks/generating-code

import * as Blockly from 'blockly/core';
import BlocklyJS from 'blockly/javascript';

BlocklyJS['text'] = function(block) {
    let textValue = block.getFieldValue('TEXT');
    return [textValue, 0]
}

/**
 *  NRC Generator
 */
BlocklyJS['forunion'] = function (block){
    const assignment = validate(block.getFieldValue('OBJECT_KEY'));
    const object = validate(block.getFieldValue( 'ATTRIBUTE_VALUE'));
    return `for ${assignment} in ${object} union \n`
}

BlocklyJS['tuple'] = function (block){
    const attributes = Blockly.JavaScript.statementToCode(block, 'ATTRIBUTES');
    return `{(${attributes})}`;
}

BlocklyJS['tuple_el'] = function (block){
    const attribute_Name = validate(block.getFieldValue('ATTRIBUTE_NAME'))
    const attribute_value = validate(BlocklyJS.valueToCode(block,'ATTRIBUTE_VALUE',0));
    return `${attribute_Name} := ${attribute_value}`;
}

BlocklyJS['customReactComponent'] = function (block){
    console.log('[customReactComponent]', block)
    const code = validate(block.getField('FIELD').getText())
    return [code,0];
}

BlocklyJS['tuple_el_iteration'] = function (block){
    const attribute_Name = validate(block.getFieldValue('ATTRIBUTE_NAME'))
    const attribute_value = validate(BlocklyJS.statementToCode(block,'ATTRIBUTE_VALUE',0));
    return `${attribute_Name} :=\n${attribute_value}`;
}

BlocklyJS['ifstmt_primitive'] = function (block){
    const object_association = validate(BlocklyJS.statementToCode(block,'OBJECT_ASSOCIATION'));
    const attributes = validate(BlocklyJS.statementToCode(block, 'ATTRIBUTES'));
    return [`if ${object_association} then\n ${attributes}`, 0];
}

// Todo see above, conditional association, then a statement 
BlocklyJS['ifstmt_bag'] = function (block){
    const object_association = validate(BlocklyJS.statementToCode(block,'OBJECT_ASSOCIATION'));
    return `if ${object_association} then\n`;
}

BlocklyJS['association_on'] = function (block){
    const object_associationA = validate(block.getFieldValue('ATTRIBUTE_A'));
    const object_associationB = validate(block.getFieldValue('ATTRIBUTE_A'));
    return `(${object_associationA} = ${object_associationB})`;
}
BlocklyJS['association_on_assisted'] = function (block){
    const object_associationA = validate(block.getFieldValue('ATTRIBUTE_A'));
    const object_associationB = validate(block.getFieldValue('ATTRIBUTE_A'));
    return `(${object_associationA} = ${object_associationB})`;
}
// add a sumby function
BlocklyJS['group_by'] = function (block){
    const attribute_key = validate(block.getFieldValue('ATTRIBUTE_KEY'))
    const attribute_value = validate(block.getFieldValue('ATTRIBUTE_VALUE'))
    return `groupBy({${attribute_key}}, {${attribute_value}})`
}
BlocklyJS['brackets'] = function (block){
    const group_by = validate(BlocklyJS.statementToCode(block,'GROUP_BY'))
    return `\n(${group_by})`
}
BlocklyJS['or'] = function (block){
    return ' || '
}
BlocklyJS['and'] = function (block){
    return ' && '
}


const validate = (codeBlock) =>{
    return codeBlock?codeBlock:"UND";
}


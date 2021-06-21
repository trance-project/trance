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
    const statements = validate(BlocklyJS.statementToCode(block, 'NRC_STATEMENT'));
    return `for ${assignment} in ${object} union \n ${statements}`
}

BlocklyJS['tuple'] = function (block){
    const attributes = Blockly.JavaScript.statementToCode(block, 'ATTRIBUTES');
    return `{(${attributes})}`;
}

BlocklyJS['tuple_el'] = function (block){
    const attribute_Name = validate(block.getFieldValue('ATTRIBUTE_NAME'))
    const attribute_value = validate(BlocklyJS.valueToCode(block,'ATTRIBUTE_VALUE',0));
    const comma = moreThenOneAttribute(block);
    return `${comma}${attribute_Name} := ${attribute_value}`;
}

BlocklyJS['customReactComponent'] = function (block){
    const code = validate(block.getField('FIELD').getText())
    return [code,0];
}

BlocklyJS['tuple_el_iteration'] = function (block){
    const attribute_Name = validate(block.getFieldValue('ATTRIBUTE_NAME'))
    const attribute_value = validate(BlocklyJS.statementToCode(block,'ATTRIBUTE_VALUE',0));
    const comma = moreThenOneAttribute(block);
    return `${comma}${attribute_Name} :=\n${attribute_value}`;
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
    const dropdown_options = block.getFieldValue('DROPDOWN_OPTIONS');
    const object_associationB = validate(block.getFieldValue('ATTRIBUTE_B'));
    return `(${object_associationA} ${dropdown_options} ${object_associationB})`;
}
BlocklyJS['association_on_assisted'] = function (block){
    const object_associationA = validate(BlocklyJS.valueToCode(block,'ATTRIBUTE_A',0));
    const dropdown_options = block.getFieldValue('DROPDOWN_OPTIONS');
    const object_associationB = validate(BlocklyJS.valueToCode(block,'ATTRIBUTE_B',0));
    return `(${object_associationA} ${dropdown_options} ${object_associationB})`;
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

/**
 * Function used to check the tuple code block to include a
 * comma if more then one attribute is selected
 * @param block
 * @returns {string|null}
 */
const moreThenOneAttribute = (block) =>{
    const parentBlock = block.parentBlock_;
    if(parentBlock){
        switch (parentBlock.type){
            case "tuple_el" :{
               return ", "
            }
            default : {
                return ""
            }
        }
    }
    return "";
}


/**
 * nrc 2.0 code generation
 */

Blockly.JavaScript['forUnion2.0'] = function(block) {
    const text_object_key = block.getFieldValue('OBJECT_KEY');
    const dropdown_dropdown_placeholder = block.getFieldValue('DROPDOWN_PLACEHOLDER');
    const statements_name = Blockly.JavaScript.statementToCode(block, 'NAME');
    // TODO: Assemble JavaScript into code variable.
    const code = '...;\n';
    return code;
};

Blockly.JavaScript['forUnionTuple2.0'] = function(block) {
    const text_object_key = block.getFieldValue('OBJECT_KEY');
    const dropdown_dropdown_placeholder = block.getFieldValue('DROPDOWN_PLACEHOLDER');
    const statements_name = Blockly.JavaScript.statementToCode(block, 'NAME');
    // TODO: Assemble JavaScript into code variable.
    const code = '...;\n';
    return code;
};

// Todo see above, conditional association, then a statement
Blockly.JavaScript['ifstmt_bag2.0'] = function (block){
    const object_association = validate(BlocklyJS.statementToCode(block,'OBJECT_ASSOCIATION'));
    return `if ${object_association} then\n`;
}

Blockly.JavaScript['forUnionNested'] = function (block){
    const assignment = validate(block.getFieldValue('OBJECT_KEY'));
    const object = validate(block.getField('FIELD').getText())
    const statements = validate(BlocklyJS.statementToCode(block, 'NRC_STATEMENT'));
    return `for ${assignment} in ${object} union \n ${statements}`
}

Blockly.JavaScript['nrc_filters'] = function(block) {
    const value_one = Blockly.JavaScript.valueToCode(block, 'INPUT_ONE', Blockly.JavaScript.ORDER_ATOMIC);
    const dropdown_operators = block.getFieldValue('OPERATORS');
    const value_two = Blockly.JavaScript.valueToCode(block, 'INPUT_TWO', Blockly.JavaScript.ORDER_ATOMIC);
    const code = `${value_one}${dropdown_operators}${value_two}`;
    return code;
};


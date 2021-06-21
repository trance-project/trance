import Blockly from 'blockly';
import BlocklyJS from 'blockly/javascript';
import "../fields/BlocklyReactField";


Blockly.defineBlocksWithJsonArray([
    {
        "type": "forunion",
        "message0": "for %1 in %2 union %3 %4",
        "args0": [
            {
                "type": "field_input",
                "name": "OBJECT_KEY",
                "text": ""
            },
            {
                "type": "input_dummy",
                "name": "DROPDOWN_PLACEHOLDER",
            },
            {
                "type": "input_dummy"
            },
            {
                "type": "input_statement",
                "name": "NRC_STATEMENT"
            }
        ],
        "extensions": ["dynamic_menu_extension"],
        "colour": 142,
        "nextStatement": "Bag",
        "previousStatement": "Bag",
        "tooltip": "For Union: Bag Type",
        "inputsInline": true,

    },
    {
        "type": "tuple_el",
        "message0": "%1 %2 %3",
        "args0": [
            {
                "type": "field_input",
                "name": "ATTRIBUTE_NAME",
                "text": ""
            },
            {
                "type": "field_label",
                "name": "COLON",
                "text": ":="
            },
            {
                "type": "input_value",
                "name": "ATTRIBUTE_VALUE",
                "check": ["Primitive" ,"Number", "String", "Boolean"]
            }
        ],
        "colour": 111,
        "previousStatement": null,
        "nextStatement": null,
        "tooltip": "tuple_el"
    },
    {
        "type": "tuple_el_iteration",
        "message0": "%1 %2 %3 %4",
        "args0": [
            {
                "type": "field_input",
                "name": "ATTRIBUTE_NAME",
                "text": ""
            },
            {
                "type": "field_label",
                "name": "COLON",
                "text": ":="
            },
            {
                "type": "input_dummy"
            },
            {
                "type": "input_statement",
                "name": "ATTRIBUTE_VALUE",
                "check": "Bag"
            }
        ],
        "colour": 230,
        "previousStatement": null,
        "nextStatement": null,
        "tooltip": "tuple_el_iteration",
    },
    {
        "type": "tuple",
        "message0": "{(%1)}",
        "args0": [
            {
                "type": "input_statement",
                "name": "ATTRIBUTES"
            }
        ],
        "colour": 255,
        "previousStatement": "Bag",
        "nextStatement": "Bag",
        "tooltip": "tuple",

    },
    {
        "type": "brackets",
        "message0": "(%1).groupBy({%2}, {%3})",
        "args0": [
            {
                "type": "input_statement",
                "name": "GROUP_BY"
            },
            {
                "type": "field_input",
                "name": "ATTRIBUTE_KEY",
                "text": ""
            },
            {
                "type": "field_input",
                "name": "ATTRIBUTE_VALUE",
                "text": ""
            }
        ],
        "colour": 33,
        "previousStatement": null,
        "nextStatement": null,
        "tooltip": "brackets",
    },
    {
        "type": "or",
        "message0": "||",
        "colour": 50,
        "previousStatement": "Primitive",
        "nextStatement": "Primitive",
        "tooltip": "or",

    },
    {
        "type": "and",
        "message0": "&&",
        "colour": 142,
        "previousStatement": "Primitive",
        "nextStatement": "Primitive",
        "tooltip": "and",

    },
    {
        "type": "ifstmt_primitive",
        "message0": "if %1 then %2",
        "args0": [
            {
                "type": "input_statement",
                "name": "OBJECT_ASSOCIATION",
                "check": "Primitive"
            },
            {
                "type": "input_statement",
                "name": "ATTRIBUTES",
                "check": "Primitive"
            }
        ],
        "colour": 23,
        "output" : "Primitive",
        // "previousStatement": "Primitive",
        // "nextStatement": "Primitive",
        "tooltip": "If Statement: Primitive Type",

    },
    {
        "type": "ifstmt_bag",
        "message0": "if %1 then",
        "args0": [
            {
                "type": "input_statement",
                "name": "OBJECT_ASSOCIATION",
                "check": "Bag"
            }
        ],
        "colour": 10,
        "previousStatement": "Bag",
        "nextStatement": "Bag",
        "tooltip": "If Statment: Bag Type",

    },
    // this should be a conditional 
    // ==, ||, &&, <, etc. in dropdown
    // Boolean type, with Primitive for now
    // was also thinking it could be 
    // a middle piece, if possible 
    {
        "type": "association_on",
        "message0": "%1 %2 %3",
        "args0": [
            {
                "type": "field_input",
                "name": "ATTRIBUTE_A",
                "text": ""
            },
            {
                "type": "field_dropdown",
                "name": "DROPDOWN_OPTIONS",
                "options": [
                    [
                        "==",
                        "=="
                    ],
                    [
                        "||",
                        "||"
                    ],
                    [
                        "&&",
                        "&&"
                    ],
                    [
                        ">",
                        ">"
                    ],
                    [
                        "<",
                        "<"
                    ]
                ]
            },
            {
                "type": "field_input",
                "name": "ATTRIBUTE_B",
                "text": ""
            }
        ],
        "colour": 142,
        // "output": "Primitive",
        "previousStatement": "Primitive",
        "nextStatement": "Primitive",
        "tooltip": "association_on",
    },
    {
        "type": "association_on_assisted",
        "message0": "(%1 %2 %3)",
        "args0": [
            {
                "type": "input_value",
                "name": "ATTRIBUTE_A",
                "text": ""
            },
            {
                "type": "field_dropdown",
                "name": "DROPDOWN_OPTIONS",
                "options": [
                    [
                        "==",
                        "=="
                    ],
                    [
                        "||",
                        "||"
                    ],
                    [
                        "&&",
                        "&&"
                    ],
                    [
                        ">",
                        ">"
                    ],
                    [
                        "<",
                        "<"
                    ]
                ]
            },
            {
                "type": "input_value",
                "name": "ATTRIBUTE_B",
                "text": ""
            }
        ],
        "inputsInline": true,
        "colour": 142,
        // "output": "Primitive",
        "nextStatement": "Bag",
        "previousStatement": "Bag",
        "tooltip": "association_on_assisted",
    },
    {
        "type": "group_by",
        "message0": "Group by %1_%2 ",
        "args0": [
            {
                "type": "field_input",
                "name": "ATTRIBUTE_KEY",
                "text": ""
            },
            {
                "type": "field_input",
                "name": "ATTRIBUTE_VALUE",
                "text": ""
            }
        ],
        "colour": 130,
        "previousStatement": "Bag",
        "nextStatement": "Bag",
        "tooltip": "group_by",
    }
]);

const testReactField = {
    "type": "customReactComponent",
    "message0": "%1",
    "args0": [
        {
            "type": "field_react_component",
            "name": "FIELD",
            "text": "select attribute",
        },
    ],
    "output": "Primitive",

};

Blockly.Blocks['customReactComponent'] = {
    init: function() {
        this.jsonInit(testReactField);
        this.setStyle('loop_blocks');
    }
};

Blockly.Blocks['new_boundary_function'] = {
    init: function () {
        this.appendDummyInput()
            .appendField(new Blockly.FieldTextInput("Boundary Function Name"), "Name");
        this.appendStatementInput("Content")
            .setCheck(null);
        this.setInputsInline(true);
        this.setColour(315);
        this.setTooltip("");
        this.setHelpUrl("");
    }
};

BlocklyJS['new_boundary_function'] = function (block) {
    const text_name = block.getFieldValue('Name');
    const statements_content = BlocklyJS.statementToCode(block, 'Content');
    // TODO: Assemble into code variable.
    const code = 'def ' + text_name + '(_object,**kwargs):\n' + statements_content + '\n';
    return code;
};


/**
 * NRC 2.0 style guide
 */

const forUnionTuple2 = {
    "type": "forUnionTuple2.0",
    "message0": "for %1 in %2 union {{ %3 %4 }}" ,
    "args0": [
        {
            "type": "field_input",
            "name": "OBJECT_KEY",
            "text": ""
        },
        {
            "type": "field_dropdown",
            "name": "DROPDOWN_PLACEHOLDER",
            "options": [
                [
                    "option",
                    "OPTIONNAME"
                ],
                [
                    "option",
                    "OPTIONNAME"
                ],
                [
                    "option",
                    "OPTIONNAME"
                ]
            ]
        },
        {
            "type": "input_dummy"
        },
        {
            "type": "input_statement",
            "name": "NAME"
        }
    ],
    "colour": 150,
    "nextStatement": "Bag",
    "previousStatement": "Bag",
    "tooltip": "For Union: Bag Type",
    "helpUrl": ""
};

Blockly.Blocks['forUnionTuple2.0'] = {
    init: function() {
        this.jsonInit(forUnionTuple2);
        this.setStyle('loop_blocks');
    }
};

const forUnion2 = {
    "type": "forUnion2.0",
    "message0": "for %1 in %2 union ( %3 %4 )" ,
    "args0": [
        {
            "type": "field_input",
            "name": "OBJECT_KEY",
            "text": ""
        },
        {
            "type": "field_dropdown",
            "name": "DROPDOWN_PLACEHOLDER",
            "options": [
                [
                    "option",
                    "OPTIONNAME"
                ],
                [
                    "option",
                    "OPTIONNAME"
                ],
                [
                    "option",
                    "OPTIONNAME"
                ]
            ]
        },
        {
            "type": "input_dummy"
        },
        {
            "type": "input_statement",
            "name": "NAME"
        }
    ],
    "colour": 315,
    "nextStatement": "Bag",
    "previousStatement": "Bag",
    "tooltip": "For Union: Bag Type",
    "helpUrl": ""
};


Blockly.Blocks['forUnion2.0'] = {
    init: function() {
        this.jsonInit(forUnion2);
    }
};

const ifstmt_bag2 = {
    "type": "ifstmt_bag2.0",
    "message0": "if %1 %2 then",
    "args0": [
        {
            "type": "input_dummy"
        },
        {
            "type": "input_statement",
            "name": "OBJECT_ASSOCIATION",
            "check": "Bag"
        }
    ],
    "mutator": "controls_if_mutatorss",
    "colour": 10,
    "previousStatement": "Bag",
    "nextStatement": "Bag",
    "tooltip": "If Statment: Bag Type",
};

Blockly.Blocks['ifstmt_bag2.0'] = {
    init: function() {
        this.jsonInit(ifstmt_bag2);
    }
};

const forUnionNested = {
    "type": "forUnionNested",
    "message0": "for %1 in %2 union",
    "args0": [
        {
            "type": "field_input",
            "name": "OBJECT_KEY",
            "text": ""
        },
        {
            "type": "field_react_component",
            "name": "FIELD",
            "text": "select attribute",
        },
    ],
    "colour": 211,
    "nextStatement": "Bag",
    "previousStatement": "Bag",
    "tooltip": "For Union Nested: Bag Type",
    "inputsInline": true,
};


Blockly.Blocks['forUnionNested'] = {
    init: function() {
        this.jsonInit(forUnionNested);
    }
};

const nrc_filters = {
    "type": "nrc_filters",
    "message0": "%1 %2 %3",
    "args0": [
    {
        "type": "input_value",
        "name": "INPUT_ONE"
    },
    {
        "type": "field_dropdown",
        "name": "OPERATORS",
        "options": [
            [
                "+",
                "ADD"
            ],
            [
                "-",
                "SUBTRACT"
            ],
            [
                "*",
                "MULTIPLY"
            ],
            [
                "/",
                "DIVISION"
            ]
        ]
    },
    {
        "type": "input_value",
        "name": "INPUT_TWO"
    }
],
    "inputsInline": true,
    "colour": 80,
    "tooltip": "",
    "helpUrl": ""
}

Blockly.Blocks['nrc_filters'] = {
    init: function() {
        this.jsonInit(nrc_filters);
    }
};

Blockly.Extensions.registerMutator('controls_if_mutatorss',
    Blockly.Constants.Logic.CONTROLS_IF_MUTATOR_MIXIN, null,
    ['or', 'and']);





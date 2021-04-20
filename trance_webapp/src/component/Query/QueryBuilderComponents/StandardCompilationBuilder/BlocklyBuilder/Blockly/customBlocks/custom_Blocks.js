import Blockly from 'blockly';

Blockly.defineBlocksWithJsonArray([{
        "type": "forunion",
        "message0": "for %1 in %2 union",
        "args0": [
            {
                "type": "field_input",
                "name": "OBJECT_KEY",
                "text": ""
            },
            {
                "type": "field_input",
                "name": "ATTRIBUTE_VALUE"
            },

        ],
        "colour": 142,
        "nextStatement": "Bag",
        "previousStatement": "Bag",
        "tooltip": "For Union: Bag Type",

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
        "message0": "(%1)",
        "args0": [
            {
                "type": "input_statement",
                "name": "GROUP_BY"
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
                "type": "field_label",
                "name": "COLON",
                "text": "=="
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
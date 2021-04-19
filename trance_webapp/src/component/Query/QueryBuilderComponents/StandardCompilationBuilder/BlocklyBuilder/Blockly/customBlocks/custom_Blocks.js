import Blockly from 'blockly';

Blockly.defineBlocksWithJsonArray([{
        "type": "for_loop",
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
        "tooltip": "for_loop",

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
        "previousStatement": "DEFAULT",
        "nextStatement":"DEFAULT",
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
        "type": "object_association",
        "message0": "if %1 then ",
        "args0": [
            {
                "type": "input_statement",
                "name": "OBJECT_ASSOCIATION",
                "check": "Primitive"
            }
        ],
        "colour": 23,
        "output" : "Primitive",
        // "previousStatement": "Primitive",
        // "nextStatement": "Primitive",
        "tooltip": "object_association",

    },
    {
        "type": "object_association_2",
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
        "tooltip": "object_association_2",

    },
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
        "previousStatement": "DEFAULT",
        "nextStatement": "DEFAULT",
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
        "previousStatement": "DEFAULT",
        "nextStatement": "DEFAULT",
        "tooltip": "group_by",
    },
    // {
    //     "type": "tuple_attr",
    //     "message0": "%1 := %2",
    //     "args0": [
    //         {
    //             "type": "field_input",
    //             "name": "ATTRIBUTE_VALUE"
    //         },
    //         {
    //             "type": "input_statement",
    //             "name": "ATTRIBUTES"
    //         }
    //     ],
    //     "colour": 33,
    //     "previousStatement": ":=",
    //     "nextStatement": ":=",
    //     "tooltip": "brackets",
    // },
    // {
    //     "type": "sng_tuple",
    //     "message0": "Sng %1",
    //     "args0": [
    //         {
    //             "type": "input_statement",
    //             "name": "SINGLETON",
    //             "check": [":="]
    //         }
    //     ],
    //     // can be null here since
    //     "previousStatement": "DEFAULT",
    //     "colour": 33,
    // },
    // {
    //     "type": "forunion",
    //     "message0": "for %1 in %2 union",
    //     "args0": [
    //         {
    //             "type": "field_input",
    //             "name": "OBJECT_KEY",
    //             "text": ""
    //         },
    //         {
    //             "type": "field_input",
    //             "name": "ATTRIBUTE_VALUE",
    //             // this isn't working for singleton
    //             "check": ["for", "Sng"]
    //         },
    //
    //     ],
    //     "colour": 142,
    //     "nextStatement": "DEFAULT",
    //     "previousStatement": "DEFAULT",
    //     "tooltip": "forunion",
    //
    // },
]);
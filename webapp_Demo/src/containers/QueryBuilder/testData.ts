import {Table} from "../../Interface/Public_Interfaces";

const testData:Table[] = [
    {name: "Sample",
        id: 110,
        abr:"s",
        columns:
            [
                {id: 111,name: "sample", enable: false, children:[]},
                {id: 112,name: "turmorsite", enable: false, children:[]},
                {id: 133,name: "treatment_outcome", enable: false, children:[]},
            ]
    },
    {name: "CopyNumber",
        id: 113,
        abr:"c",
        columns:
            [
                {id: 114,name: "sample", enable: false, children:[]},
                {id: 115,name: "gene", enable: false, children:[]} ,
                {id: 116,name: "cnum", enable: false, children:[]},
            ]
    },
    {name: "Occurrences",
        id: 117,
        abr:"o",
        columns:
            [
                {id: 118,name: "sample", enable: false, children:[]},
                {id: 119,name: "contig", enable: false, children:[]} ,
                {id: 120,name: "end", enable: false, children:[]},
                {id: 121,name: "reference", enable: false, children:[]},
                {id: 122,name: "alternate", enable: false, children:[]},
                {id: 123,name: "mutid", enable: false, children:[]},
                {id: 124,name: "candidates", enable: false, children:[
                        {name: "candidates",
                            id: 125,
                            columns:
                                [
                                    {id: 126,name: "gene", enable: true, children:[]},
                                    {id: 127,name: "impact", enable: true, children:[]},
                                    {id: 128,name: "sift", enable: true, children:[]},
                                    {id: 129,name: "poly", enable: true, children:[]},
                                    {id: 130,name: "consequences", enable: true, children:[
                                            {name: "consequences",
                                                id: 131,
                                                columns:
                                                    [
                                                        {id: 132,name: "conseq", enable: true, children:[]}
                                                    ]
                                            }
                                        ]},
                                ]
                        }
                    ]},
            ]
    },
    {name: "GeneExpression",
        id: 14,
        columns:
            [
                {id: 7,name: "id", enable: true, children:[]},
                {id: 8,name: "cname", enable: true, children:[]} ,
                {id: 9,name: "corders", enable: true, children:[]},
                {id: 10,name: "cdescription", enable: true, children:[]},
                {id: 11,name: "csequence", enable: true, children:[]}
            ]
    },
    {name: "ProteinProteinInteractions",
        id: 12,
        columns:
            [
                {id: 13,name: "id", enable: true, children:[]},
                {id: 14,name: "odate", enable: true, children:[]} ,
                {id: 15,name: "oparts", enable: true, children:[]},
                {id: 16,name: "odescription", enable: true, children:[]},
                {id: 17,name: "oclerk", enable: true, children:[]}
            ]
    },
    {name: "DrugResponse",
        id: 19,
        columns:
            [
                {id: 20,name: "id", enable: true, children:[]},
                {id: 21,name: "nname", enable: true, children:[]} ,
                {id: 22,name: "ncusts", enable: true, children:[]},
                {id: 23,name: "ncountry", enable: true, children:[]}
            ]
    },
    {name: "Enhancers",
        id: 24,
        columns:
            [
                {id: 25,name: "id", enable: true, children:[]},
                {id: 26,name: "rname", enable: true, children:[]} ,
                {id: 27,name: "rnation", enable: true, children:[]},
            ]
    },
    {name: "Methylation",
        id: 28,
        columns:
            [
                {id: 29,name: "id", enable: true, children:[]},
                {id: 30,name: "qty", enable: true, children:[]} ,
                {id: 31,name: "orderid", enable: true, children:[]},
                {id: 32,name: "price", enable: true, children:[]},
                {id: 33,name: "deliveryoption", enable: true, children:[]},
            ]
    },
    {name: "Genes",
        id: 34,
        columns:
            [
                {id: 35,name: "id", enable: true, children:[]},
                {id: 36,name: "pname", enable: true, children:[]},
                {id: 37,name: "pdescription", enable: true, children:[]},
                {id: 38,name: "pmanufacturer", enable: true, children:[]},
            ]
    },
    {name: "Pathways",
        id: 39,
        columns:
            [
                {id: 40,name: "id", enable: true, children:[]},
                {id: 41,name: "qty", enable: true, children:[]},
                {id: 42,name: "price", enable: true, children:[]},
                {id: 43,name: "description", enable: true, children:[]},
            ]
    },
    {name: "Variants",
        id: 44,
        columns:
            [
                {id: 45,name: "id", enable: true, children:[]},
                {id: 46,name: "sname", enable: true, children:[]},
                {id: 47,name: "sdescription", enable: true, children:[]},
                {id: 48,name: "sprice", enable: true, children:[]},
            ]
    }
]

export default testData;
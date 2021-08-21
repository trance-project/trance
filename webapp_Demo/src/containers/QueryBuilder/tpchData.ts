import {Table} from "../../Interface/Public_Interfaces";

const tpchData:Table[] = [
    {name: "Customer",
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
    {name: "Order",
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
    {name: "Nation",
        id: 19,
        columns:
            [
                {id: 20,name: "id", enable: true, children:[]},
                {id: 21,name: "nname", enable: true, children:[]} ,
                {id: 22,name: "ncusts", enable: true, children:[]},
                {id: 23,name: "ncountry", enable: true, children:[]}
            ]
    },
    {name: "Region",
        id: 24,
        columns:
            [
                {id: 25,name: "id", enable: true, children:[]},
                {id: 26,name: "rname", enable: true, children:[]} ,
                {id: 27,name: "rnation", enable: true, children:[]},
            ]
    },
    {name: "Lineitem",
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
    {name: "Part",
        id: 34,
        columns:
            [
                {id: 35,name: "id", enable: true, children:[]},
                {id: 36,name: "pname", enable: true, children:[]},
                {id: 37,name: "pdescription", enable: true, children:[]},
                {id: 38,name: "pmanufacturer", enable: true, children:[]},
            ]
    },
    {name: "Partsupp",
        id: 39,
        columns:
            [
                {id: 40,name: "id", enable: true, children:[]},
                {id: 41,name: "qty", enable: true, children:[]},
                {id: 42,name: "price", enable: true, children:[]},
                {id: 43,name: "description", enable: true, children:[]},
            ]
    },
    {name: "Supplier",
        id: 44,
        columns:
            [
                {id: 45,name: "id", enable: true, children:[]},
                {id: 46,name: "sname", enable: true, children:[]},
                {id: 47,name: "sdescription", enable: true, children:[]},
                {id: 48,name: "sprice", enable: true, children:[]},
            ]
    },
    {name: "COP",
        id: 49,
        columns:
            [
                {id: 50,name: "cname", enable: true, children:[]},
                {id: 51,name: "corders", enable: true, children:[
                        {name: "Order",
                            id: 52,
                            columns:
                                [
                                    {id: 53,name: "id", enable: true, children:[]},
                                    {id: 54,name: "odate", enable: true, children:[]} ,
                                    {id: 56,name: "odescription", enable: true, children:[]},
                                    {id: 57,name: "oclerk", enable: true, children:[]},
                                    {id: 55,name: "oparts", enable: true, children:[
                                            {name: "Part",
                                                id: 58,
                                                columns:
                                                    [
                                                        {id: 59,name: "id", enable: true, children:[]},
                                                        {id: 60,name: "pname", enable: true, children:[]},
                                                        {id: 61,name: "pdescription", enable: true, children:[]},
                                                        {id: 62,name: "pmanufacturer", enable: true, children:[]},
                                                    ]
                                            }
                                        ]}
                                ]
                        }
                    ]},
            ]
    }
]
// {name: "Table 1",
//     id: 17,
//     columns:
//     [
//         {id: 18,
//             name: "Column_1(NestedObject)",
//             enable: true,
//             children:[{
//                 name: "Column_1(NestedObject)",
//                 id: 16,
//                 columns: [
//                     {id: 1, name:"child_column_1", enable: true, children:[]},
//                     {id: 2,name:"child_column_2", enable: true, children:[]},
//                     {id: 3,name:"child_column_3(NestedObject)", enable: true, children:[
//                             {name: "child_column_3(NestedObject)",
//                                 id: 15,
//                                 columns: [
//                                     {id: 4,name:"sub_child_column_1", enable: true, children:[]},
//                                 ]}
//                         ]}
//                 ]}]
//         },
//         {id: 5,name: "Column 2", enable: true, children:[]},
//         {id: 6,name: "Column 3", enable: true, children:[]}
//     ]
// },
export default tpchData;
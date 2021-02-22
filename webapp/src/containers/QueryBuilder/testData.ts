import {Table} from "../../Interface/Public_Interfaces";

const testData:Table[] = [
    {name: "Table 1",
        id: 17,
        columns:
            [
                {id: 18, name: "Column_1(NestedObject)", enable: true,
                    children:[{
                        name: "Column_1(NestedObject)",
                        id: 16,
                        columns: [
                            {id: 1, name:"child_column_1", enable: true, children:[]},
                            {id: 2,name:"child_column_2", enable: true, children:[]},
                            {id: 3,name:"child_column_3(NestedObject)", enable: true, children:[
                                    {name: "child_column_3(NestedObject)",
                                        id: 15,
                                        columns: [
                                            {id: 4,name:"sub_child_column_1", enable: true, children:[]},
                                        ]}
                                ]}
                        ]}]
                },
                {id: 5,name: "Column 2", enable: true, children:[]},
                {id: 6,name: "Column 3", enable: true, children:[]}
            ]
    },
    {name: "Table 2",
        id: 14,
        columns:
            [
                {id: 7,name: "Column 1", enable: true, children:[]},
                {id: 8,name: "Column 2", enable: true, children:[]} ,
                {id: 9,name: "Column 3", enable: true, children:[]}
            ]
    },
    {name: "Table 3",
        id: 13,
        columns:
            [
                {id: 10,name: "Column 1", enable: true, children:[]},
                {id: 11,name: "Column 2", enable: true, children:[]},
                {id: 12,name: "Column 3", enable: true, children:[]}
            ]
    }
]

export default testData;
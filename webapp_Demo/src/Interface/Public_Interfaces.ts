import React from "react";

export interface Table {
    id:number;
    name:string;
    columns : Column[];
    abr?:string;
    join?:Table;
}

export interface Join {
    tables:Table[];

}

export interface Column {
    id: number;
    name: string;
    enable: boolean;
    children: Table[]
}

export interface Query {
    type: "select"|"join",
    level: string;
    tables: Table[];
    Where: string;
    groupBy: string;
    selectedColumns:Column[];
    associations: Associations[];
    children?: Query
}

export interface customTabElement {
    tabLabel:string;
    jsxElement: React.ReactNode,
    disable?:boolean
}

export interface planDemoOutput{
    sample: string;
    mutations:mutation[];
}

export interface mutation{
    mutId:string;
    scores:score[];
}

export interface score{
    gene:string;
    score:number;
}

export interface Associations {
    key: number;
    label: string[];
}
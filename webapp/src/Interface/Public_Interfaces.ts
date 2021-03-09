import React from "react";

export interface Table {
    id:number;
    name:string;
    columns : Column[];
    abr?:string;
}

export interface Column {
    id: number;
    name: string;
    enable: boolean;
    children: Table[]
}

export interface Query {
    tables: Table[];
    Where: string;
    groupBy: string;
    selectedColumns:Column[];
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
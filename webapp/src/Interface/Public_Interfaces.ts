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
    jsxElement: React.ReactNode
}

import React from "react";

export interface Table {
    id:number;
    name:string;
    columns : Column[];
    abr?:string;
}

export interface TempTable {
    _id:string;
    name:string;
    abr?:string;
    columns : TempColumn[];
}

export interface TempColumn {
    name: string;
}

export interface Column {
    id: number;
    name: string;
    tableAssociation: string;
    enable: boolean;
    children: Table;
}

export interface BlocklyNrcCode {
    title: string;
    body: string;
}

export interface QuerySummaryList {
    id: string;
    date: string;
    name:string;
    tables: string;
    groupedBy: string;
}

export interface Column {
    id: number;
    name: string;
    tableAssociation: string;
    enable: boolean;
    children: Table;
}

export interface Query {
    name: String;
    level: string;
    table: Table;
    Where?: string;
    groupBy?: GroupBy;
    selectedColumns?:Column[];
    associations?: Association[];
    children?: Query;
    filters?: string[];
}

export interface GroupBy {
    type: "groupBy" | "sumBy"
    key: string
}

export interface customTabElement {
    tabLabel:string;
    jsxElement: React.ReactNode,
    disable?:boolean
}


export interface score{
    gene:string;
    score:number;
}

export interface Association {
    key: string;
    association: ObjectAssociation[];
}

export interface ObjectAssociation {
    objectAssociation: [Column, Column];
    objects: Table[]
}

export type LabelAssociation = {
    join: string;
    tables?: Table[];
}


export interface NewQuery {
    name?: string;
    key: string;
    labels? : NewQuery[]
}




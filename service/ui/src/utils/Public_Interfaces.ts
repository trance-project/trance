import React from "react";
import Blockly, {Xml} from "blockly/blockly";
import xml = Blockly.utils.xml;
// import {RawNodeDatum} from "react-d3-tree/lib/types/common";

export interface Table {
    id:number;
    name:string;
    columns : Column[];
    abr?:string;
}

export interface TempTable {
    _id:string;
    name:string;
    isCollectionType: boolean,
    abr?:string;
    columns : TempColumn[];
}

export interface TempColumn {
    name: string;
    dataType: "String" | "Collection" | "Boolean" | "Number";
}

export interface Column {
    id: number;
    name: string;
    tableAssociation: string;
    enable: boolean;
    children: Table;
}

export interface BlocklyNrcCode {
    _id?: string;
    title: string;
    body: string;
}

export interface QuerySummary {
    _id?: string;
    date?: string;
    name:string;
    lastRunQueryURL?:string
    xmlDocument: string;
    tables?: string;
    groupedBy?: string;
}

export interface Column {
    id: number;
    name: string;
    tableAssociation: string;
    enable: boolean;
    children: Table;
}

export interface Query {
    id?: string;
    name: string;
    level: string;
    table?: Table;
    Where?: string;
    groupBy?: GroupBy;
    selectedColumns?:Column[];
    associations?: Association[];
    children?: Query;
    filters?: string[];
    BlocklyDocument?: string;
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

export interface Plan {
    name: string;
    plan: RawNodeDatum;
}

export interface QueryResponse {
    nrc: NewQuery[];
}

export interface RawNodeDatum {
    name: string;
    attributes?: Record<string, string>;
    children?: RawNodeDatum[];
}
export interface ShreddedResponse {
    shred_plan: RawNodeDatum[],
    shred_nrc: NewQuery[]
}

export interface StandardResponse {
    standard_plan: RawNodeDatum[]
}

export interface LabelType {
    for: string;
    tuple: string[];
    association: string;
    groupBy: string;
}

export interface NotepadResponse {
    nodepad_url: string;
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

export interface RunTimeMetrics{
    pid: number,
    shred_write_size: number,
    stand_write_size: number,
}

export type AbstractTable = {
    name: string;
    columnNames: string[];
    subTables?: AbstractTable;
}

export interface TableGraphMetaInfo {
    id: number;
    count:number;
}




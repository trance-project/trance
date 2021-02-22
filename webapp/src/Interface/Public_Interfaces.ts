export interface Table {
    id:number;
    name:string;
    columns : Column[];
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
}
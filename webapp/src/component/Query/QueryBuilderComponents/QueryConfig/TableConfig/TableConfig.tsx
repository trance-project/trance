import React, {useState} from 'react';
import {TreeView} from "@material-ui/lab";

import MinusSquare from "../../../../ui/ExpandedIcons/MinusSquare";
import PlusSquare from "../../../../ui/ExpandedIcons/PlusSquare";
import CloseSquare from "../../../../ui/ExpandedIcons/CloseSquare";
import StyledTreeItem from "../../../../ui/StyledTreeItem/StyledTreeItem";
import tableConfigThemeStyle from "./TableConfigThemeStyle";
import ItemLabel from "../../../../ui/StyledTreeItem/ItemLabel/ItemLabel";
import {Column, Table} from "../../../../../Interface/Public_Interfaces";

interface _TableConfigProps{
    tables: Table[];
    columnBoxClicked: (column:Column) => void;
}

const TableConfig = (props: _TableConfigProps) => {
    const [expanded, setExpanded] = useState<string[]>([]);

    const keepExpanded = (nodeId:string) => {
        const e = [...expanded];
        e.push(nodeId);
        setExpanded(e);
        console.log(expanded)
    };

    //Recursive method used to iterate over query table object to layout the columns and if the supply nested data.
    const createParentTreeItem = (table:Table) =>{
        const nodeIdString=table.id.toString();
        return (<StyledTreeItem key={Math.random().toString()}
                                nodeId={nodeIdString}
                                label={<ItemLabel checkBoxClicked={() =>{}} labelText={table.name} />}
                                onClick={()=>keepExpanded(nodeIdString)}
                                >
            {
                table.columns.map((column, index) => (
                    createChildTreeItem(table, column)
                ))
            }
        </StyledTreeItem>
    )};

    const createChildTreeItem = (table:Table, column: Column) => {
        if(column.children.length > 0){
            return column.children.map(table => createParentTreeItem(table));
        }else{
            const nodeIdString=column.id.toString();
            return (
                <StyledTreeItem key={Math.random().toString()}
                                nodeId={nodeIdString}
                                label={<ItemLabel checkBoxClicked={() => props.columnBoxClicked(column)} labelText={column.name} checkBoxEnabled={column.enable} isColumn/>}
                                onClick={()=>keepExpanded(nodeIdString)}
                />
            )
        }
    };
    const classes = tableConfigThemeStyle();

    const treeItem = props.tables.map((table) => createParentTreeItem(table));

    return(
        <TreeView
            className={classes.root}
            defaultExpanded={['1']}
            defaultCollapseIcon={<MinusSquare />}
            defaultExpandIcon={<PlusSquare />}
            defaultEndIcon={<CloseSquare/>}
            expanded={expanded}
            >
            {treeItem}
        </TreeView>


    );
}

export default TableConfig;


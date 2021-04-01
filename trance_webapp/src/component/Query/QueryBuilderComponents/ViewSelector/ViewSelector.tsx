import React from "react";

import {ListItem, ListItemText} from "@material-ui/core";
import {Table} from "../../../../utils/Public_Interfaces";
import { FixedSizeList, ListChildComponentProps } from 'react-window';

interface _ViewSelectorProps{
    tables: Table[];
    clicked: (table:Table) => void;

}

const ViewSelector = (props:_ViewSelectorProps) => {
    const itemCount = props.tables.length;
    const tables = props.tables;

    function renderRow(rowProps: ListChildComponentProps) {
        const { index, style } = rowProps;
        const labelId = `checkbox-list-secondary-label-${index}`;
        return (
            <ListItem button style={style} key={index}>
                <ListItemText id={labelId} primary={tables[index].name} onClick={() => props.clicked(tables[index])}/>
            </ListItem>
        );
    }

    return (
        <FixedSizeList height={400} width={300} itemSize={46} itemCount={itemCount}>
                {renderRow}
        </FixedSizeList>
    );
}

export default ViewSelector;
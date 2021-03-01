import React from 'react';
import Typography from "@material-ui/core/Typography";

import TableConfig from './TableConfig/TableConfig';
import ConditionContrainsts from './ConditionContrainsts/ConditionContrainsts';
import {Column, customTabElement, Query} from "../../../Interface/Public_Interfaces";
import CustomTabs from "../../ui/CustomTabs/CustomTabs";





interface _QueryConfigProps {
    query: Query | undefined;
    config: (column:Column) => void;
}

const QueryConfig = (props: _QueryConfigProps) => {
    const tabPanel:customTabElement[] | undefined = props.query ? [
        {
            tabLabel:"Table Config",
            jsxElement:<TableConfig tables={props.query.tables} columnBoxClicked={props.config}/>
        },
        {
            tabLabel:"Table Config",
            jsxElement:<ConditionContrainsts/>
        },
        {
            tabLabel:"Group By",
            jsxElement:<Typography>Group By to be constructed</Typography>
        },
        {
            tabLabel:"Table Joins",
            jsxElement:<Typography>Table Joins to be constructed</Typography>
        },
    ]: undefined;

    return tabPanel ? <CustomTabs tabsElement={tabPanel}/> : <div></div>

}

export default QueryConfig;
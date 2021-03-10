import React from 'react';
import Typography from "@material-ui/core/Typography";

import TableConfig from './TableConfig/TableConfig';
import ConditionContrainsts from './ConditionContrainsts/ConditionContrainsts';
import {Column, customTabElement, Query} from "../../../../Interface/Public_Interfaces";
import CustomTabs from "../../../ui/CustomTabs/CustomTabs";
import JoinConfig from "./TableConfig/JoinConfig/JoinConfig";





interface _QueryConfigProps {
    query: Query | undefined;
    config: (column:Column) => void;
}

const QueryConfig = (props: _QueryConfigProps) => {
    const tabPanel:customTabElement[] | undefined = props.query ? [
        {
            tabLabel:"Input Config",
            jsxElement:<TableConfig tables={props.query.tables} columnBoxClicked={props.config}/>
        },
        {
            tabLabel:"Filters",
            jsxElement:<ConditionContrainsts/>
        },
        {
            tabLabel:"Projections",
            jsxElement:<Typography>Projections Attributes to be constructed</Typography>
        },
        {
            tabLabel:"Group By",
            jsxElement:<Typography>Group By to be constructed</Typography>
        },
        {
            tabLabel:"Joins",
            jsxElement:<JoinConfig/>
        },
    ]: undefined;

    return tabPanel ? <CustomTabs tabsElement={tabPanel}/> : <div></div>

}

export default QueryConfig;
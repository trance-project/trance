import React from 'react';

import TableConfig from './TableConfig/TableConfig';
import ConditionContrainsts from './ConditionContrainsts/ConditionContrainsts';
import {Column, customTabElement, Query} from "../../../../utils/Public_Interfaces";
import CustomTabs from "../../../ui/CustomTabs/CustomTabs";





interface _QueryConfigProps {
    query: Query;
    config: (column:Column) => void;
    selectedNode: string;
}


const QueryConfig = (props: _QueryConfigProps) => {
    const node = parseInt(props.selectedNode);
    let nestedLvl = 0;
    let queryObject:Query = props.query;

    const selectedQuery = (query:Query) => {
        nestedLvl++;
        if(node === nestedLvl){
            console.log("setting query in config " + nestedLvl + " " + node)
            queryObject = query;
        }else{
            console.log("not found " + nestedLvl + " " + node)
            if(query.children)
            selectedQuery(query.children)
        }

    }
    selectedQuery(props.query);

    const tabPanel:customTabElement[] | undefined = queryObject ? [
        {
            tabLabel:"Input Config",
            jsxElement:<TableConfig table={queryObject.table} columnBoxClicked={props.config}/>
        },
        {
            tabLabel:"Filter",
            jsxElement:<ConditionContrainsts/>
        },
        {
            tabLabel:"Group By",
            jsxElement:<div></div> //<GroupByConfig onClickGroup={()=>{}}/>
        },
        {
            tabLabel:"Association",
            jsxElement: <div></div>//<JoinConfig/>
        },
    ]: undefined;

    return tabPanel ? <CustomTabs tabsElement={tabPanel}/> : <div></div>

}

export default QueryConfig;
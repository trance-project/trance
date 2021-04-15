/**
 * @Author Brandon Moore
 * @Date 30 March 2021
 * This component is used display the selectedQuery object into source NRC code
 */

import React from 'react';
import {TreeView} from "@material-ui/lab";


import {
    Association,
    Query,
    Table,
    ObjectAssociation,
    LabelAssociation,
    NewQuery
} from "../../../utils/Public_Interfaces";
import StyledTreeItem from "../../ui/StyledTreeItem/StyledTreeItem";
import MinusSquare from "../../ui/ExpandedIcons/MinusSquare";
import PlusSquare from "../../ui/ExpandedIcons/PlusSquare";
import CloseSquare from "../../ui/ExpandedIcons/CloseSquare";
import LabelView from "../../Query/QueryBuilderComponents/StandardCompilationBuilder/LabelView/LabelView";
import {standardCompilationViewThemeStyle} from './standardCompilationViewThemeStyle';
import NewLabelView from "../../Query/QueryBuilderComponents/StandardCompilationBuilder/LabelView/NewLabelView";


interface _QueryViewProps {
    query:Query | undefined;
    showDiagram:boolean;
    closeDiagram:()=>void;
    hoverMaterializationLvl:number;
    hoverMaterializationLvlClose: ()=>void;
    hoverMaterializationLvlOpen:(index:number)=>void;
    abortHover:()=>void;
}

type LabelType = {
    for: string;
    tuple: string[];
    association: string;
    groupBy: string;
}


const StandardCompilationView = (props:_QueryViewProps) => {
    const classes = standardCompilationViewThemeStyle();

    const newQuerySelected =  {
        // name: "QuerySimple",
        key: "For s in samples Union ",
        labels: [{
            name: "sample",
            key: "s.bcr_patient_uuid"
        }, {
            name: "mutations",
            key: "For o in occurrences Union If (s.bcr_patient_uuid = o.donorId) Then ",
            labels: [{
                name: "mutId",
                key: "o.oid",
            },{
                name : "scores",
                key : "ReduceByKey[gene], [score], For t in o.transcript_consequences Union For c in copynumber Union If (t.gene_id = c.cn_gene_id AND c.cn_aliquot_uuid = s.bcr_aliquot_uuid) Then ",
                labels : [{
                    name: "gene",
                    key : "t.gene_id"
                }, {
                    name : "score",
                    key : "((c.cn_copy_number + 0.01) * If (t.impact = HIGH) Then 0.8 Else If (t.impact = MODERATE) Then 0.5 Else If (t.impact = LOW) Then 0.3 Else 0.01   )"
                } ]
            }]
        }]
    }  as NewQuery

    let statement = <div></div>;

    const expandedNode: string[] = [];

    //ShallowCopy of parent Association to be used in child node
    let shallowAssociation: Association[] | undefined;

    //Recursive method used to iterate over query table object to layout the columns and if the supply nested data.
    const newQuerySelect = (query:NewQuery) => {
        const nodeId = (expandedNode.length + 1).toString();
        expandedNode.push(nodeId);

        const childItem: JSX.Element[] = [];
        const labelViewString: string[] = [];
        let label: LabelType = {
            for: "",
            association: "",
            tuple: [],
            groupBy: "",
        };
        labelViewString.push(query.key)
        if(query.key.includes("ReduceByKey") || query.key.includes("GroupByKey") || query.key.includes("SumByKey")){
            label.groupBy=query.key.substring(query.key.indexOf("ReduceByKey"), query.key.indexOf(" For "))
            label.for = query.key.substring(query.key.indexOf(" For "), query.key.length)
        }else{
            label.for=query.key;
        }

        if(query.labels){
            query.labels.forEach(q => {
                if(q.labels){
                    //new Node!
                    label.tuple.push(`${q.name} := `)
                    childItem.push(newQuerySelect(q));
                }else{
                    label.tuple.push(`${q.name} := ${q.key}`)
                }
            })
            return (
                <StyledTreeItem
                    key={`${nodeId}_${query.key}`}
                    nodeId={nodeId}
                    label={
                        <NewLabelView
                            labelView={label}
                        />}>
                    {childItem}
                </StyledTreeItem>
            )
        }else{
            return <StyledTreeItem key={`${nodeId}_${query.key}`}
                                   nodeId={nodeId}
                                   label={<NewLabelView
                                       labelView={label}
                                   />}/>
        }

        }



    //Recursive method used to iterate over query table object to layout the columns and if the supply nested data.
    // const querySelect = (query:Query) => {
    //     const nodeId = query.level;
    //     expandedNode.push(nodeId);
    //
    //     const childItem: JSX.Element[] = [];
    //     let columnsSelect: string[] = [];
    //     let table = query.table;
    //     let tableAbr = table.abr?table.abr:"UDF";
    //     let labelAssociation: LabelAssociation = {
    //         join:"",
    //         tables: []
    //     };
    //     if(shallowAssociation){
    //         for(const association of(shallowAssociation)){
    //             labelAssociation.join = association.association.map(el => {
    //                 el.objectAssociation.forEach(col => {
    //                     if(col.children){
    //                         labelAssociation.tables!.push(col.children);
    //                     }
    //                 })
    //                 return checkAssociation(el);
    //             }).join(' && ');
    //         }
    //         //reset shallowAssociation
    //         shallowAssociation = undefined;
    //     }
    //
    //
    //     columnsSelect.push(...checkColumnsEnable(query.table));
    //     if(query.filters){
    //         columnsSelect.push(...query.filters);
    //     }
    //     if(query.associations){
    //         shallowAssociation = query.associations;
    //         for(const association of(query.associations)){
    //             columnsSelect.push(`${association.key}:=`);
    //         }
    //     }
    //
    //     if(query.children){
    //         childItem.push(querySelect(query.children));
    //
    //         return (
    //             <StyledTreeItem
    //                 key={table.id}
    //                 nodeId={nodeId}
    //                 label={
    //                     <LabelView
    //                         tableEl={tableAbr}
    //                         tableName={table.name}
    //                         columns={columnsSelect}
    //                         association={labelAssociation}
    //                     />}>
    //                 {childItem}
    //             </StyledTreeItem>
    //         )
    //     }else{
    //         return <StyledTreeItem key={table.id}
    //                                nodeId={nodeId}
    //                                label={<LabelView
    //                                    tableEl={tableAbr}
    //                                    tableName={table.name}
    //                                    columns={columnsSelect}
    //                                    association={labelAssociation}
    //                                />}/>
    //     }
    // }

    /**
     * @Params columns
     * method used to return only columns that are enabled
     */
    const checkColumnsEnable = (table: Table) =>{
        let tableAbr = table.abr?table.abr:"UDF";
        const columnsSelected: string[] = [];
        for(const column of(table.columns)){
            if(column.enable){
                const columnName = `${tableAbr}.${column.name}`;
                columnsSelected.push(`${column.name}:=${columnName}`);
            }
        }
        return columnsSelected;
    }

    /**
     * Method used to filter the Association and verify Association
     * @param association
     */
    const checkAssociation=(association: ObjectAssociation) => {
            const associationString: string[] = [];
        association.objectAssociation.forEach(column => {
            if(column.children){
               associationString.push(columnChildrenFilter(column.children));
            }else{
                associationString.push(`${column.tableAssociation}.${column.name}`);
            }

        });
        return associationString.join(' == ');
    }

    const columnChildrenFilter = (table: Table) => {
        let columnAssociation: string = "";
        table.columns.forEach(column => {
            if(column.children){
                columnChildrenFilter(column.children);
            }else{
                columnAssociation =`${column.tableAssociation}.${column.name}`;
            }
        });
        return columnAssociation;
    }



    if(props.query) {
            statement=   newQuerySelect(newQuerySelected);
        }

    console.log('[nodeID]', expandedNode);
    return (
        <div>
            <TreeView
            // defaultExpanded={expandedNode}
            defaultCollapseIcon={<MinusSquare/>}
            defaultEndIcon={<CloseSquare />}
            defaultExpandIcon={<PlusSquare/>}
            >
     {statement}
    {/*    /!*Example set*!/*/}

            </TreeView>
        </div>
    );
};

const replaceBetween = ( string: number, end: number, replaceString: string) =>{
    return
}

export default StandardCompilationView;






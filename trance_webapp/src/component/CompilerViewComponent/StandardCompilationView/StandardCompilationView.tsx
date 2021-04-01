/**
 * @Author Brandon Moore
 * @Date 30 March 2021
 * This component is used display the selectedQuery object into source NRC code
 */

import React from 'react';
import {TreeView} from "@material-ui/lab";


import {Association, Query, Table,ObjectAssociation, LabelAssociation} from "../../../utils/Public_Interfaces";
import StyledTreeItem from "../../ui/StyledTreeItem/StyledTreeItem";
import MinusSquare from "../../ui/ExpandedIcons/MinusSquare";
import PlusSquare from "../../ui/ExpandedIcons/PlusSquare";
import CloseSquare from "../../ui/ExpandedIcons/CloseSquare";
import LabelView from "../../Query/QueryBuilderComponents/StandardCompilationBuilder/LabelView/LabelView";
import {standardCompilationViewThemeStyle} from './standardCompilationViewThemeStyle';


interface _QueryViewProps {
    query:Query | undefined;
    showDiagram:boolean;
    closeDiagram:()=>void;
    hoverMaterializationLvl:number;
    hoverMaterializationLvlClose: ()=>void;
    hoverMaterializationLvlOpen:(index:number)=>void;
    abortHover:()=>void;
}


const StandardCompilationView = (props:_QueryViewProps) => {
    const classes = standardCompilationViewThemeStyle();

    let statement = <div></div>;

    const expandedNode: string[] = [];

    //ShallowCopy of parent Association to be used in child node
    let shallowAssociation: Association[] | undefined;


    //Recursive method used to iterate over query table object to layout the columns and if the supply nested data.
    const querySelect = (query:Query) => {
        const nodeId = query.level;
        expandedNode.push(nodeId);

        const childItem: JSX.Element[] = [];
        let columnsSelect: string[] = [];
        let table = query.table;
        let tableAbr = table.abr?table.abr:"UDF";
        let labelAssociation: LabelAssociation = {
            join:"",
            tables: []
        };
        if(shallowAssociation){
            for(const association of(shallowAssociation)){
                labelAssociation.join = association.association.map(el => {
                    el.objectAssociation.forEach(col => {
                        if(col.children){
                            labelAssociation.tables!.push(col.children);
                        }
                    })
                    return checkAssociation(el);
                }).join(' && ');
            }
            //reset shallowAssociation
            shallowAssociation = undefined;
        }


        columnsSelect.push(...checkColumnsEnable(query.table));
        if(query.filters){
            columnsSelect.push(...query.filters);
        }
        if(query.associations){
            shallowAssociation = query.associations;
            for(const association of(query.associations)){
                columnsSelect.push(`${association.key}:=`);
            }
        }

        if(query.children){
            childItem.push(querySelect(query.children));

            return (
                <StyledTreeItem
                    key={table.id}
                    nodeId={nodeId}
                    label={
                        <LabelView
                            tableEl={tableAbr}
                            tableName={table.name}
                            columns={columnsSelect}
                            association={labelAssociation}
                        />}>
                    {childItem}
                </StyledTreeItem>
            )
        }else{
            return <StyledTreeItem key={table.id}
                                   nodeId={nodeId}
                                   label={<LabelView
                                       tableEl={tableAbr}
                                       tableName={table.name}
                                       columns={columnsSelect}
                                       association={labelAssociation}
                                   />}/>
        }
    }

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
            statement=   querySelect(props.query);
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

export default StandardCompilationView;




// <StyledTreeItem nodeId="1"
//                 label={
//                     <LabelView
//                         tableEl={'s'}
//                         tableName={'Samples'}
//                         columns={["same:= s.sample", "mutations:=" ]}
//                         hoverEvent={()=>props.hoverMaterializationLvlOpen(1)}
//                         abortHover={props.abortHover}
//                     />}
// >
//     <StyledTreeItem nodeId="2"
//                     label={
//                         <LabelView
//                             tableEl={'o'}
//                             tableName={'Occurrences'}
//                             joinString={"s.sample == o.sample"}
//                             columns={["mutId := o.mutId", "scores :=" ]}
//                             hoverEvent={()=>props.hoverMaterializationLvlOpen(2)}
//                             abortHover={props.abortHover}/>
//                     }>
//         <StyledTreeItem nodeId="3"
//                         label={
//                             <LabelView
//                                 tableEl={'c'}
//                                 nestedObjectEl={"t"}
//                                 nestedObjectJoin={"o.candidates"}
//                                 tableName={'CopyNumber'}
//                                 joinString={'t.gene == c.gene && s.sample == o.sample'}
//                                 columns={["gene := t.gene","score := t.impact * (c.num + 0.01) * t.sift * t.poly"]}
//                                 sumBy
//                                 hoverEvent={()=>props.hoverMaterializationLvlOpen(3)}
//                                 abortHover={props.abortHover}
//                             />}>
//         </StyledTreeItem>
//     </StyledTreeItem>
// </StyledTreeItem>
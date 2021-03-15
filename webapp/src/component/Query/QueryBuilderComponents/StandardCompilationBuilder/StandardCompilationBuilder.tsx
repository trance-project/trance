import React, {useRef, useEffect,useState} from 'react';
import {TreeView} from "@material-ui/lab";


import { Query, Table} from "../../../../Interface/Public_Interfaces";
import StyledTreeItem from "../../../ui/StyledTreeItem/StyledTreeItem";
import MinusSquare from "../../../ui/ExpandedIcons/MinusSquare";
import PlusSquare from "../../../ui/ExpandedIcons/PlusSquare";
import CloseSquare from "../../../ui/ExpandedIcons/CloseSquare";
import StringIdGenerator from "../../../../classObjects/stringIdGenerator";
import LabelView from '../../LabelView/LabelView';

interface _QueryBuilderProps {
    query:Query | undefined;
}


const StandardCompilationBuilder = (props:_QueryBuilderProps) => {
    const stringIdGen = StringIdGenerator.getInstance()!;
    const [selectedNodeState, setSelectedNodeState] = useState("1");
    let statement = <div></div>;

    const focusNodeRef = useRef<HTMLButtonElement>(null);

    const handleSelectedNode = (nodeId:string) => setSelectedNodeState(nodeId);


    useEffect(()=>{
        if(focusNodeRef.current){
            focusNodeRef.current.click();
        }
    }, []);



    //Recursive method used to iterate over query table object to layout the columns and if the supply nested data.
    // const querySelect = (table:Table) => {
    //     if(!table.abr){
    //         table.abr = stringIdGen.next();
    //     }
    //     let tableAbr =table.abr;
    //     let childItem: JSX.Element[] = [<div></div>];
    //     let columnsSelect: string[] = [];
    //     if(containsNestedObject(table)){
    //         for(const column of(table.columns)){
    //             if(column.children.length > 0){
    //                 childItem= column.children.map(t => querySelect(t));
    //             }
    //             if(column.enable){
    //                 const columnName = column.children.length===0?`${tableAbr}.${column.name}`:"";
    //                 columnsSelect.push(`${column.name}:=${columnName}`);
    //             }
    //         }
    //         return (
    //             <StyledTreeItem nodeId={table.id.toString()} label={<LabelView tableEl={tableAbr} tableName={table.name} columns={columnsSelect}/>}>
    //                 {childItem}
    //             </StyledTreeItem>
    //         )
    //     }else{
    //         for(const column of(table.columns)){
    //             if(column.enable){
    //                 const columnName = column.children.length===0?`${tableAbr}.${column.name}`:"";
    //                 columnsSelect.push(`${column.name}:=${columnName}`);
    //             }
    //         }
    //         return <StyledTreeItem nodeId="2" label={<LabelView tableEl={tableAbr} tableName={table.name} columns={columnsSelect}/>}/>
    //
    //     }
    // }

    const containsNestedObject = (table:Table) =>{
        return table.columns.some(c => c.children.length>0)
    }


    if(props.query) {
        // for(const table of props.query.tables){
        //     statement=   querySelect(table);
        // }
    }

    return (
        <div>
            <TreeView
                expanded={['1','2', '3']}
                defaultCollapseIcon={<MinusSquare/>}
                defaultEndIcon={<CloseSquare />}
                defaultExpandIcon={<PlusSquare/>}
            >
                <StyledTreeItem nodeId="1"
                                label={
                                    <LabelView
                                        tableEl={'s'}
                                        tableName={'Samples'}
                                        columns={["sample:= s.sample", "mutations:=" ]}
                                        ref={focusNodeRef}
                                        selectNode={()=>handleSelectedNode("1")}
                                        isSelected={selectedNodeState==="1"}
                                    />}
                >
                    <StyledTreeItem nodeId="2"
                                    label={
                                        <LabelView
                                            tableEl={'o'}
                                            tableName={'Occurrences'}
                                            joinString={"s.sample == o.sample"}
                                            columns={["mutId := o.mutId", "scores :=" ]}
                                            selectNode={()=>handleSelectedNode("2")}
                                            isSelected={selectedNodeState==="2"}
                                        />
                                    }>
                        <StyledTreeItem nodeId="3"
                                        label={
                                            <LabelView
                                                sumBy
                                                tableEl={'t'}
                                                tableName={'o.candidates'}
                                                selectNode={()=>handleSelectedNode("3")}
                                                isSelected={selectedNodeState==="3"}
                                            />}>
                            <StyledTreeItem nodeId="4" label={
                                <LabelView tableEl={'c'}
                                           tableName={'CopyNumber'}
                                           joinString={'t.gene == c.gene && o.sample == c.sample'}
                                           columns={["gene := t.gene","score := t.impact * (c.num + 0.01) * t.sift * t.poly)}))})}"]}
                                           selectNode={()=>handleSelectedNode("4")}
                                           isSelected={selectedNodeState==="4"}
                                />}/>
                        </StyledTreeItem>
                    </StyledTreeItem>
                </StyledTreeItem>
            </TreeView>
        </div>
    );
};


export default StandardCompilationBuilder;
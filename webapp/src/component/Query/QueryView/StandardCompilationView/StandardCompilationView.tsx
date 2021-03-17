import React from 'react';
import {TreeView} from "@material-ui/lab";

import { Query, Table} from "../../../../Interface/Public_Interfaces";
import StyledTreeItem from "../../../ui/StyledTreeItem/StyledTreeItem";
import MinusSquare from "../../../ui/ExpandedIcons/MinusSquare";
import PlusSquare from "../../../ui/ExpandedIcons/PlusSquare";
import CloseSquare from "../../../ui/ExpandedIcons/CloseSquare";
import LabelView from "../../LabelView/LabelView";
import StringIdGenerator from "../../../../classObjects/stringIdGenerator";
import TreeDiagram from "../../../ui/TreeDiagram/TreeDiagram";
import {RawNodeDatum} from "react-d3-tree/lib/types/common";
import AccountTreeIcon from '@material-ui/icons/AccountTree';
import {IconButton, Popover} from "@material-ui/core";
import {standardCompilationViewThemeStyle} from './standardCompilationViewThemeStyle';
import Materializationlvl1 from "../../QueryBuilderComponents/QueryShredding/Materialzation/Materializationlvl1";
import Materializationlvl2 from "../../QueryBuilderComponents/QueryShredding/Materialzation/Materializationlvl2";
import Materializationlvl3 from "../../QueryBuilderComponents/QueryShredding/Materialzation/Materializationlvl3";

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
    const stringIdGen = StringIdGenerator.getInstance()!;
    let statement = <div></div>;


    //Recursive method used to iterate over query table object to layout the columns and if the supply nested data.
    const querySelect = (table:Table) => {
        if(!table.abr){
            table.abr = stringIdGen.next();
        }
        let tableAbr =table.abr;
        let childItem: JSX.Element[] = [<div></div>];
        let columnsSelect: string[] = [];
        if(containsNestedObject(table)){
            for(const column of(table.columns)){
                if(column.children.length > 0){
                    childItem= column.children.map(t => querySelect(t));
                }
                if(column.enable){
                    const columnName = column.children.length===0?`${tableAbr}.${column.name}`:"";
                    columnsSelect.push(`${column.name}:=${columnName}`);
                }
            }
            return (
                <StyledTreeItem nodeId={table.id.toString()} label={<LabelView tableEl={tableAbr} tableName={table.name} columns={columnsSelect}/>}>
                    {childItem}
                </StyledTreeItem>
            )
        }else{
            for(const column of(table.columns)){
                if(column.enable){
                    const columnName = column.children.length===0?`${tableAbr}.${column.name}`:"";
                    columnsSelect.push(`${column.name}:=${columnName}`);
                }
            }
            return <StyledTreeItem nodeId="2" label={<LabelView tableEl={tableAbr} tableName={table.name} columns={columnsSelect}/>}/>

        }
    }

    const containsNestedObject = (table:Table) =>{
         return table.columns.some(c => c.children.length>0)
    }


    if(props.query) {
        for(const table of props.query.tables){
            statement=   querySelect(table);
        }
    }

    return (
        <div>
            <TreeView
            defaultExpanded={['1','2', '3']}
            defaultCollapseIcon={<MinusSquare/>}
            defaultEndIcon={<CloseSquare />}
            defaultExpandIcon={<PlusSquare/>}
            >
     {/*{statement}*/}
    {/*    /!*Example set*!/*/}
            <StyledTreeItem nodeId="1"
                            label={
                                <LabelView
                                    tableEl={'s'}
                                    tableName={'Samples'}
                                    columns={["same:= s.sample", "mutations:=" ]}
                                    hoverEvent={()=>props.hoverMaterializationLvlOpen(1)}
                                    abortHover={props.abortHover}
                                />}
            >
                <Popover
                    open={props.hoverMaterializationLvl === 1}
                    onClose={props.hoverMaterializationLvlClose}
                    anchorOrigin={{
                        vertical: 'top',
                        horizontal: 'right',
                    }}
                    transformOrigin={{
                        vertical: 'top',
                        horizontal: 'right',
                    }}
                >
                    <Materializationlvl1/>
                </Popover>
                <StyledTreeItem nodeId="2"
                                label={
                                    <LabelView
                                        tableEl={'o'}
                                        tableName={'Occurrences'}
                                        joinString={"s.sample == o.sample"}
                                        columns={["mutId := o.mutId", "scores :=" ]}
                                        hoverEvent={()=>props.hoverMaterializationLvlOpen(2)}
                                        abortHover={props.abortHover}/>
                                }>
                    <Popover
                        open={props.hoverMaterializationLvl === 2}
                        onClose={props.hoverMaterializationLvlClose}
                        anchorOrigin={{
                            vertical: 'top',
                            horizontal: 'right',
                        }}
                        transformOrigin={{
                            vertical: 'top',
                            horizontal: 'right',
                        }}
                    >
                        <Materializationlvl2/>
                    </Popover>
                    <StyledTreeItem nodeId="3"
                                    label={
                                        <LabelView
                                            tableEl={'c'}
                                            nestedObjectEl={"t"}
                                            nestedObjectJoin={"o.candidates"}
                                            tableName={'CopyNumber'}
                                            joinString={'t.gene == c.gene && s.sample == o.sample'}
                                            columns={["gene := t.gene","score := t.impact * (c.num + 0.01) * t.sift * t.poly"]}
                                            sumBy
                                            hoverEvent={()=>props.hoverMaterializationLvlOpen(3)}
                                            abortHover={props.abortHover}
                                        />}>
                        <Popover
                            open={props.hoverMaterializationLvl === 3}
                            onClose={props.hoverMaterializationLvlClose}
                            anchorOrigin={{
                                vertical: 'top',
                                horizontal: 'right',
                            }}
                            transformOrigin={{
                                vertical: 'top',
                                horizontal: 'right',
                            }}
                        >
                            <Materializationlvl3/>
                        </Popover>
                    </StyledTreeItem>
                </StyledTreeItem>
            </StyledTreeItem>
            </TreeView>
        </div>
    );
};

const treeDiagramData:RawNodeDatum ={
    name: 'Sample,mutations',
    attributes: {
        level: '1',
    },
    children: [
        {
            name: 'mutId, candidates, sID, sample',
            attributes: {
                level: '2',
            },
            children: [
                {
                    name: 'gene,score,sID,sample,mutId',
                    attributes: {
                        level: '2',
                    },
                    children: [
                        {
                            name: 'impact*(cnum+0.01)*sift*polysID',
                            attributes: {
                                newLine:'sample, label, gene',
                                level: '3',
                            },
                            children:[
                                {
                                    name:'sample.gene',
                                    attributes: {
                                        level: '3',
                                    },
                                    children:[
                                        {
                                            name:'candidates',
                                            attributes: {
                                                level: '3',
                                            },
                                            children:[
                                                {
                                                    name:'sample',
                                                    attributes: {
                                                        level: '2',
                                                    },
                                                    children:[
                                                        {name: 'Occurrences',
                                                            attributes: {
                                                                level: '1',
                                                            }},
                                                        {name:'Samples',
                                                            attributes: {
                                                                level: '1',
                                                            }}
                                                    ]
                                                }
                                            ]
                                        },
                                        {
                                            name:'CopyNumber',
                                            attributes: {
                                                level: '3',
                                            }
                                        }
                                    ]
                                }
                            ]
                        },
                    ],
                },
            ],
        },
    ],
};

export default StandardCompilationView;
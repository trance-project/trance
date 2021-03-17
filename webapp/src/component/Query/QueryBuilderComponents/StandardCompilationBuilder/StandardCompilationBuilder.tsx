import React, {useRef, useEffect} from 'react';
import {TreeView} from "@material-ui/lab";


import {Associations, Query, Table} from "../../../../Interface/Public_Interfaces";
import StyledTreeItem from "../../../ui/StyledTreeItem/StyledTreeItem";
import MinusSquare from "../../../ui/ExpandedIcons/MinusSquare";
import PlusSquare from "../../../ui/ExpandedIcons/PlusSquare";
import CloseSquare from "../../../ui/ExpandedIcons/CloseSquare";
import StringIdGenerator from "../../../../classObjects/stringIdGenerator";
import LabelView from '../../LabelView/LabelView';
import TableJoinDialog from "./DialogTableConfigWizard/TableJoinDialog/TableJoinDialog";
import TableJoinPane from "./DialogTableConfigWizard/TableJoinDialog/TableJoinPane";
import QueryEditDialog from "./DialogTableConfigWizard/QueryEditDialog/QueryEditDialog";
import GroupByDialog from "./DialogTableConfigWizard/GroupByDialog/GroupByDialog";

interface _QueryBuilderProps {
    query:Query;
    joinAction: (table:Table)=>void;
    selectedNode: string;
    onNodeClick: (nodeId: string) => void

    associations: Query;
    onAssociation: (objectAssociations:string[]) => void;
    onAssociationDelete: (key:number)=>void;

}


const StandardCompilationBuilder = (props:_QueryBuilderProps) => {
    const stringIdGen = StringIdGenerator.getInstance()!;
    const [openJoinDialogState, setOpenJoinDialogState] = React.useState(false);
    const [openJoinPaneState, setOpenJoinPaneState] = React.useState(false);
    const [openEditDialogState, setOpenEditDialogState] = React.useState(false);
    const [openGroupByDialogState, setOpeGroupDialogState] = React.useState(false);
    const [tables, setTables] = React.useState<string[]>([])

    const [lvl1AssociationKey, setLvl1AssociationKey] = React.useState<string>("")
    const [lvl2AssociationKey, setLvl2AssociationKey] = React.useState<string>("")
    const [lvl3Edit, setLvl3Edit] = React.useState<string>("gene:=t.gene")
    const [lvl3GroupBy, setLvl3GroupBy] = React.useState<string>("")

    const handleAssociationChange = (input: React.ChangeEvent<HTMLInputElement>) => {
        if(props.selectedNode === "1"){
            setLvl1AssociationKey(input.target.value);
        }else if(props.selectedNode === "2"){
            setLvl2AssociationKey(input.target.value);
        }
    }

    const handleLvl3GroupByChange = (input: React.ChangeEvent<HTMLInputElement>) => {
        setLvl3GroupBy(input.target.value);
    }

    const handleLvl3EditChange = (input: React.ChangeEvent<HTMLInputElement>) => {
        setLvl3Edit(input.target.value);
    }

    const handleClickOpenGroupByDialogState = () => {
        setOpeGroupDialogState(true);
    };

    const handleCloseGroupByDialogState = () => {
        setOpeGroupDialogState(false);
    };

    const handleClickOpenEditDialogState = () => {
        setOpenEditDialogState(true);
    };

    const handleCloseEditDialogState = () => {
        setOpenEditDialogState(false);
    };

    const handleClickOpenJoinPaneState = () => {
        setOpenJoinPaneState(true);
    };

    const handleCloseJoinPaneState = () => {
        setOpenJoinPaneState(false);
    };

    const handleClickOpenJoinDialog = () => {
        setOpenJoinDialogState(true);
    };

    const handleCloseJoinDialog = () => {
        setOpenJoinDialogState(false);
        handleClickOpenJoinPaneState();
    };

    const handleJoinAction = (table:Table) => {
        handleCloseJoinDialog()
        if(props.selectedNode==="2"){
            if(props.query.children?.tables.length!>0){
                setTables([table.name, props.query.children?.tables[0].name!])
            }
        }else{
            if(props.query.tables.length>0){
                setTables([table.name, props.query.tables[0].name])
            }
        }
        props.joinAction(table);
    }

    let _nodeId = 1;
    const expandedNodes:string[] = [];

    const nextNodeId = () => {
        const currentId = _nodeId;
        _nodeId++;
        expandedNodes.push(currentId.toString());
        return currentId.toString();
    }

    const scope = () => {
        if(expandedNodes.length === 1){
            return ")}";
        }else if(expandedNodes.length === 2){
            return ")})}";
        }else if(expandedNodes.length === 3) {
            return ")})})}";
        }
    }

    let statement = <div></div>;

    const focusNodeRef = useRef<HTMLButtonElement>(null);


    useEffect(()=>{
        if(focusNodeRef.current){
            focusNodeRef.current.click();
        }
    }, []);



    //Recursive method used to iterate over query table object to layout the columns and if the supply nested data.
    const querySelect = (query:Query) => {
        const table = query.tables[0]!;
        let associationNestedKey = "";
        if(table.name==="Sample" && lvl1AssociationKey.length> 0){
            associationNestedKey=lvl1AssociationKey + " := ";
        }else if(table.name==="Occurrences" && lvl2AssociationKey.length> 0){
            associationNestedKey=lvl2AssociationKey + " := ";
        }
        let association: string = "";
        let tableAbr =table.abr!;
        let childItem: JSX.Element[] = [];
        let columnsSelect: string[] = [];
        const treeNodeId=nextNodeId();

        if(query.associations){
            association= query.associations.map(a => a.label.join(" == ")).join(" && ")
        }
        // if(table.join){
        if(query.children){
            childItem.push(querySelect(query.children!));
            for(const column of table.columns){
                    if(column.enable){
                        const columnName = column.children.length===0?`${tableAbr}.${column.name}`:"";
                        columnsSelect.push(`${column.name}:=${columnName}`);
                    }
            }
            if(associationNestedKey.length>0){
                columnsSelect.push(associationNestedKey)
            }
            return (
                <StyledTreeItem nodeId={treeNodeId} label={<LabelView
                    tableEl={tableAbr}
                    tableName={table.name}
                    columns={columnsSelect}
                    selectNode={()=>props.onNodeClick(treeNodeId)}
                    isSelected={props.selectedNode===treeNodeId}
                    openJoinAction={handleClickOpenJoinDialog}
                    joinString={association}
                    openEdit={handleClickOpenEditDialogState}
                />}>
                    {childItem}
                </StyledTreeItem>
            )
        }
        else {
                for (const column of (table.columns)) {
                    if (column.enable) {
                        const columnName = column.children.length === 0 ? `${tableAbr}.${column.name}` : "";
                        columnsSelect.push(`${column.name}:=${columnName}`);
                    }
                }
            if(associationNestedKey.length>0){
                columnsSelect.push(associationNestedKey)
            }
            if (table.name==="CopyNumber") {
                if(columnsSelect.length>0){
                    console.log('lvl3Edit', lvl3Edit.split(","))
                    columnsSelect = lvl3Edit.split(",");
                }else{
                    // setLvl3Edit(columnsSelect.join(" , "));
                }

                return <StyledTreeItem nodeId={treeNodeId} label={<LabelView
                    tableEl={tableAbr}
                    nestedObjectEl={"t"}
                    nestedObjectJoin={"o.candidates"}
                    tableName={table.name}
                    columns={columnsSelect}
                    joinString={association}
                    // joinString={'t.gene == c.gene'}
                    selectNode={() => props.onNodeClick(treeNodeId)}
                    isSelected={props.selectedNode === treeNodeId}
                    openJoinAction={handleClickOpenJoinDialog}
                    openEdit={handleClickOpenEditDialogState}
                    openGroupBy={handleClickOpenGroupByDialogState}
                    sumBy={lvl3GroupBy.length>0}
                    endScope={scope()}
                />}/>
            } else {
                return <StyledTreeItem nodeId={treeNodeId} label={<LabelView
                    tableEl={tableAbr}
                    tableName={table.name}
                    columns={columnsSelect}
                    joinString={association}
                    selectNode={() => props.onNodeClick(treeNodeId)}
                    isSelected={props.selectedNode === treeNodeId}
                    openJoinAction={handleClickOpenJoinDialog}
                    openEdit={handleClickOpenEditDialogState}
                    endScope={scope()}
                />}/>
            }
        }
    }



    if(props.query.tables.length>0) {
        // for(const table of props.query.tables){
            statement=   querySelect(props.query);
        // }
    }

    let joins: Associations[] = [];
    if(props.selectedNode === "1"){
        if(props.query.children)
        joins =  props.query.children.associations
    }else if(props.selectedNode === "2") {
        if(props.query.children?.children)
        joins =  props.query.children!.children!.associations
    }

    return (
        <div>
            <TreeView
                expanded={expandedNodes}
                defaultCollapseIcon={<MinusSquare/>}
                defaultEndIcon={<CloseSquare />}
                defaultExpandIcon={<PlusSquare/>}
            >
                {statement}
            </TreeView>
            <TableJoinDialog
                close={handleCloseJoinDialog}
                open={openJoinDialogState}
                joinOnClock={handleJoinAction}
                associationKeyChange={handleAssociationChange}
                keyValue={props.selectedNode === "1"?lvl1AssociationKey:lvl2AssociationKey}
            />
            <TableJoinPane
                open={openJoinPaneState}
                close={handleCloseJoinPaneState}
                associations={joins}
                objects={tables}
                onAssociation={props.onAssociation}
                onAssociationDelete={props.onAssociationDelete}
            />
            <QueryEditDialog open={openEditDialogState} close={handleCloseEditDialogState} value={lvl3Edit} onChangeEdit={handleLvl3EditChange}/>
            <GroupByDialog close={handleCloseGroupByDialogState} open={openGroupByDialogState} groupByKey={lvl3GroupBy} onChangeGroupBy={handleLvl3GroupByChange}/>
        </div>
    );
};



export default StandardCompilationBuilder;



// <TreeView
//     expanded={['1','2', '3']}
//     defaultCollapseIcon={<MinusSquare/>}
//     defaultEndIcon={<CloseSquare />}
//     defaultExpandIcon={<PlusSquare/>}
// >
//     <StyledTreeItem nodeId="1"
//                     label={
//                         <LabelView
//                             tableEl={'s'}
//                             tableName={'Samples'}
//                             columns={["sample:= s.sample", "mutations:=" ]}
//                             ref={focusNodeRef}
//                             selectNode={()=>handleSelectedNode("1")}
//                             isSelected={selectedNodeState==="1"}
//                         />}
//     >
//         <StyledTreeItem nodeId="2"
//                         label={
//                             <LabelView
//                                 tableEl={'o'}
//                                 tableName={'Occurrences'}
//                                 joinString={"s.sample == o.sample"}
//                                 columns={["mutId := o.mutId", "scores :=" ]}
//                                 selectNode={()=>handleSelectedNode("2")}
//                                 isSelected={selectedNodeState==="2"}
//                             />
//                         }>
//             <StyledTreeItem nodeId="3"
//                             label={
//                                 <LabelView
//                                     sumBy
//                                     tableEl={'t'}
//                                     tableName={'o.candidates'}
//                                     selectNode={()=>handleSelectedNode("3")}
//                                     isSelected={selectedNodeState==="3"}
//                                 />}>
//                 <StyledTreeItem nodeId="4" label={
//                     <LabelView tableEl={'c'}
//                                tableName={'CopyNumber'}
//                                joinString={'t.gene == c.gene && o.sample == c.sample'}
//                                columns={["gene := t.gene","score := t.impact * (c.num + 0.01) * t.sift * t.poly)}))})}"]}
//                                selectNode={()=>handleSelectedNode("4")}
//                                isSelected={selectedNodeState==="4"}
//                     />}/>
//             </StyledTreeItem>
//         </StyledTreeItem>
//     </StyledTreeItem>
// </TreeView>
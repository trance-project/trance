import React, {useRef, useEffect} from 'react';
import {TreeView} from "@material-ui/lab";


import {Association, Query, Table} from "../../../../utils/Public_Interfaces";
import StyledTreeItem from "../../../ui/StyledTreeItem/StyledTreeItem";
import MinusSquare from "../../../ui/ExpandedIcons/MinusSquare";
import PlusSquare from "../../../ui/ExpandedIcons/PlusSquare";
import CloseSquare from "../../../ui/ExpandedIcons/CloseSquare";
import LabelView from './LabelView/LabelView';
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

    const handleLvl3EditChange = (input: React.ChangeEvent<HTMLInputElement>) => {
        setLvl3Edit(input.target.value);
    }

    const handleClickOpenGroupByDialogState = () => {
        setOpeGroupDialogState(true);
    };

    const handleCloseGroupByDialogState = () => {
        setLvl3GroupBy("scene_Group");
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
            if(props.query.children?.table){
                setTables([table.name, props.query.children?.table.name!])
            }
        }else{
            if(props.query.table){
                setTables([table.name, props.query.table.name])
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
        const table = query.table!;
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
            // association= query.associations.map(a => a.label.join(" == ")).join(" && ")
        }
        // if(table.join){
        if(query.children){
            childItem.push(querySelect(query.children!));
            for(const column of table.columns){
                    if(column.enable){
                        const columnName = column.children?`${tableAbr}.${column.name}`:"";
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
                    association={{join: association}}
                    openEdit={handleClickOpenEditDialogState}
                />}>
                    {childItem}
                </StyledTreeItem>
            )
        }
        else {
                for (const column of (table.columns)) {
                    if (column.enable) {
                        const columnName = column.children? `${tableAbr}.${column.name}` : "";
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
                    association={{join:association}}
                    // joinString={'t.gene == c.gene'}
                    selectNode={() => props.onNodeClick(treeNodeId)}
                    isSelected={props.selectedNode === treeNodeId}
                    openJoinAction={handleClickOpenJoinDialog}
                    openEdit={handleClickOpenEditDialogState}
                    openGroupBy={handleClickOpenGroupByDialogState}
                    // sumBy={lvl3GroupBy.length>0}
                    endScope={scope()}
                />}/>
            } else {
                return <StyledTreeItem nodeId={treeNodeId} label={<LabelView
                    tableEl={tableAbr}
                    tableName={table.name}
                    columns={columnsSelect}
                    association={{join: association}}
                    selectNode={() => props.onNodeClick(treeNodeId)}
                    isSelected={props.selectedNode === treeNodeId}
                    openJoinAction={handleClickOpenJoinDialog}
                    openEdit={handleClickOpenEditDialogState}
                    endScope={scope()}
                />}/>
            }
        }
    }



    if(props.query.table) {
        // for(const table of props.query.tables){
            statement=   querySelect(props.query);
        // }
    }

    let joins: Association[] = [];
    // if(props.selectedNode === "1"){
    //     if(props.query.children)
    //     joins =  props.query.children.associations
    // }else if(props.selectedNode === "2") {
    //     if(props.query.children?.children)
    //     joins =  props.query.children!.children!.associations
    // }

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
            <GroupByDialog close={handleCloseGroupByDialogState} open={openGroupByDialogState}/>
        </div>
    );
};
export default StandardCompilationBuilder;
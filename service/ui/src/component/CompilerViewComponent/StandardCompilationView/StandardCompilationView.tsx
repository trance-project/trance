/**
 * @Author Brandon Moore
 * @Date 30 March 2021
 * This component is used display the selectedQuery object into source NRC code
 */

import React from 'react';
import {TreeView} from "@material-ui/lab";
import {Popover} from "@material-ui/core";

import {
    NewQuery
} from "../../../utils/Public_Interfaces";
import StyledTreeItem from "../../ui/StyledTreeItem/StyledTreeItem";
import MinusSquare from "../../ui/ExpandedIcons/MinusSquare";
import PlusSquare from "../../ui/ExpandedIcons/PlusSquare";
import CloseSquare from "../../ui/ExpandedIcons/CloseSquare";
import NewLabelView from "../../Query/QueryBuilderComponents/StandardCompilationBuilder/LabelView/NewLabelView";
import {useAppSelector} from '../../../redux/Hooks/hooks'
import Materializationlvl1 from "../ShreddedCompilationView/Materialzation/Materializationlvl1";
import Materializationlvl2 from "../ShreddedCompilationView/Materialzation/Materializationlvl2";
import Materializationlvl3 from "../ShreddedCompilationView/Materialzation/Materializationlvl3";


interface _QueryViewProps {
    query:NewQuery | undefined;
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
    // const classes = standardCompilationViewThemeStyle();

    // const [openMaterializationState, setOpenMaterializationState] = useState<boolean>(false);

    const newQuerySelected = useAppSelector(state => state.query.responseQuery);

    let statement = <div></div>;

    const expandedNode: string[] = [];

    const _getHoverMaterializationLvl = (index: number) =>{
        switch (index){
            case 1: {
                return <Materializationlvl1/>;
            }
            case 2: {
                return <Materializationlvl2/>;
            }
            case 3: {
                return <Materializationlvl3/>;
            }
        }
    }

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
                        <React.Fragment>
                            <NewLabelView
                                labelView={label}
                                hoverEvent={()=>props.hoverMaterializationLvlOpen(parseInt(nodeId))}
                                abortHover={props.abortHover}
                            />
                            <Popover
                                open={props.hoverMaterializationLvl === parseInt(nodeId)}
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
                                {_getHoverMaterializationLvl(props.hoverMaterializationLvl)}
                            </Popover>
                        </React.Fragment>
                    }>
                    {childItem}
                </StyledTreeItem>
            )
        }else{
            return <StyledTreeItem key={`${nodeId}_${query.key}`}
                                   nodeId={nodeId}
                                   label={
                                       <React.Fragment>
                                           <NewLabelView
                                               labelView={label}
                                               hoverEvent={()=>props.hoverMaterializationLvlOpen(parseInt(nodeId))}
                                               abortHover={props.abortHover}
                                           />
                                           <Popover
                                               open={props.hoverMaterializationLvl === parseInt(nodeId)}
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
                                               {_getHoverMaterializationLvl(props.hoverMaterializationLvl)}
                                           </Popover>
                                       </React.Fragment>}/>
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



    if(newQuerySelected) {
            statement=   newQuerySelect(newQuerySelected);
        }

    return (
        <div>
            <TreeView
            defaultCollapseIcon={<MinusSquare/>}
            defaultEndIcon={<CloseSquare />}
            defaultExpandIcon={<PlusSquare/>}
            expanded={expandedNode}
            >
     {statement}

            </TreeView>
        </div>
    );
};

export default StandardCompilationView;






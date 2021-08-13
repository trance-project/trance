import React from "react";
import ExpandMoreIcon from '@material-ui/icons/ExpandMore';
import shreddedCompilationViewThemeStyle from './shreddedCompilationViewThemeStyle';
import {Accordion, AccordionDetails, AccordionSummary, Popover, Typography} from "@material-ui/core";
import Grid from "@material-ui/core/Grid";
import S from '../../ui/Span/S';
import {useAppSelector} from '../../../redux/Hooks/hooks';
import {NewQuery} from "../../../utils/Public_Interfaces";
import StyledTreeItem from "../../ui/StyledTreeItem/StyledTreeItem";
import NewLabelView from "../../Query/QueryBuilderComponents/StandardCompilationBuilder/LabelView/NewLabelView";
import {TreeView} from "@material-ui/lab";
import MinusSquare from "../../ui/ExpandedIcons/MinusSquare";
import CloseSquare from "../../ui/ExpandedIcons/CloseSquare";
import PlusSquare from "../../ui/ExpandedIcons/PlusSquare";

interface _MaterializationProps {

}

type LabelType = {
    for: string;
    tuple: string[];
    association: string;
    groupBy: string;
}

const NewShreddedCompilationView = (props:_MaterializationProps) => {
    const classes = shreddedCompilationViewThemeStyle();
    const nrcResponse = useAppSelector(state => state.query.shreddedResponse);
    const expandedNode: string[] = [];
    let statement: JSX.Element[] = [];

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
                            />
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
                                           />
                                       </React.Fragment>}/>
        }

    }

    if(nrcResponse){
     statement =   nrcResponse.shred_nrc.map((nrc,i)=>(
            <TreeView
                key={`${nrc.name}_${i}`}
                defaultCollapseIcon={<MinusSquare/>}
                defaultEndIcon={<CloseSquare />}
                defaultExpandIcon={<PlusSquare/>}
                expanded={expandedNode}
            >
                {newQuerySelect(nrc)}
            </TreeView>
        ))
    }
    return (
        <div>
            {statement}
        </div>
)
}



export default NewShreddedCompilationView
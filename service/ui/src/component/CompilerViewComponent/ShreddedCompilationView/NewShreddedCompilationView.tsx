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

    const newQuerySelect = (query:NewQuery, index: number) => {
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
        if (query.key.includes("ReduceByKey") || query.key.includes("GroupByKey") || query.key.includes("SumByKey")) {
            label.groupBy = query.key.substring(query.key.indexOf("ReduceByKey"), query.key.indexOf(" For "))
            label.for = query.key.substring(query.key.indexOf(" For "), query.key.length)
        } else {
            label.for = query.key;
        }

        return (
            <Accordion className={classes.accordion} defaultExpanded key={`${nodeId}_${query.key}`} style={{backgroundColor: _colorPicker(index)} }>
                <AccordionDetails>
                    <NewLabelView
                        labelView={label}
                    />
                </AccordionDetails>
            </Accordion>
        )
    }


    if(nrcResponse){
     statement =   nrcResponse.shred_nrc.map((nrc,i)=>(
                newQuerySelect(nrc, i)
        ))
    }
    return (
        <div>
            {statement}
        </div>
)
}

    const _colorPicker =(level:number )=>{
        switch (level) {
            case 0:
                return "rgba(141,158,145,0.8)"; //"#8D9E91";
            case 1:
                return "rgba(0, 140, 212, 0.7)"; //"rgba(139,122,140,0.3)"; //"#8B7A8C";
            case 2:
                return "rgba(179,130,181,0.8)"; //"#B382B5";
            case 3:
                return "rgba(243,71,246,0.8)";
            case 4:
                return "rgba(5,93,165,0.8)";
            case 5:
                return "rgba(40,165,5,0.8)";
            case 6:
                return "rgba(141,5,165,0.8)";
            case 7:
                return "rgba(189,148,9,0.8)";
            case 8:
                return "rgba(188,212,24,0.8)";
            case 9:
                return "rgba(5,165,120,0.8)";
        }
    }

export default NewShreddedCompilationView
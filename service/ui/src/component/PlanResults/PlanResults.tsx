import React from "react";

import {planResultsThemeStyle} from './planResultsThemeStyle';
import {
    Accordion,
    AccordionDetails,
    AccordionSummary,
    Grid,
    Typography
} from "@material-ui/core";

import {CustomNodeElementProps, RawNodeDatum} from "react-d3-tree/lib/types/common";
import Tree from "react-d3-tree";
import ShreddedPlan from "../CompilerViewComponent/ShreddedPlan/ShreddedPlan";
import ExpandMoreIcon from "@material-ui/icons/ExpandMore";

import S from "../ui/Span/S";
import projection from "../../static/images/planOperator/Projection.png";
import nest from "../../static/images/planOperator/Nest.png";
import sumAggregate from "../../static/images/planOperator/Sum_aggregate.png";
import leftOuterJoin from "../../static/images/planOperator/LeftOuterJoin.png";
import unnest from "../../static/images/planOperator/Unnest.png";
import equiJoin from "../../static/images/planOperator/Equijoin.png";
import {useAppSelector} from '../../redux/Hooks/hooks';
import {LabelType, NewQuery} from "../../utils/Public_Interfaces";
import NewLabelView from "../Query/QueryBuilderComponents/StandardCompilationBuilder/LabelView/NewLabelView";



interface _PlanResultsProps{

}

const PlanResult = (props:_PlanResultsProps) => {
    const newQuerySelect = (query:NewQuery, index: number) => {
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
                    childItem.push(newQuerySelect(q, index));
                }else{
                    label.tuple.push(`${q.name} := ${q.key}`)
                }
            })
            return (
                <React.Fragment>
                    <Accordion defaultExpanded key={`${query.key}`} style={{backgroundColor: _colorPicker(index)} }>
                        <AccordionDetails>
                            <NewLabelView
                                labelView={label}
                            />
                        </AccordionDetails>
                    </Accordion>
                    {childItem}
                </React.Fragment>
            )
        }else{
            return (
                <Accordion defaultExpanded key={`_${query.key}`} style={{backgroundColor: _colorPicker(index)} }>
                    <AccordionDetails>
                        <NewLabelView
                            labelView={label}
                        />
                    </AccordionDetails>
                </Accordion>)
        }

    }

    const classes = planResultsThemeStyle();
    const jsxElement:JSX.Element[] = [];
    const shreddedResponse = useAppSelector(state => state.query.shreddedResponse);
    const [planLength, nrcLength] = [shreddedResponse!.shred_plan.length, shreddedResponse!.shred_nrc.length];
    if(planLength === nrcLength){
        for(let i = 0; i < planLength; i++){
            const planNode = shreddedResponse!.shred_plan[i];
            const nrcNode = shreddedResponse!.shred_nrc[i];
            const depth = getNestedDepth(planNode);
            jsxElement.push((
                <React.Fragment>
                    <Grid item xs={12} md={6} style={{height: `${(depth * 100)}px`}}>
                            <Tree
                                data={planNode}
                                orientation={"vertical"}
                                pathFunc={"straight"}
                                zoom={0.3}
                                enableLegacyTransitions
                                translate={{x:200, y:20}}
                                transitionDuration={1500}
                                renderCustomNodeElement={diagramProps => renderNodeWithCustomEvents(diagramProps, i)}
                                separation={{siblings: 3.25}}
                                zoomable={false}
                            />
                    </Grid>
                    <Grid item xs={12} md={6}>
                        {newQuerySelect(nrcNode, i)}
                    </Grid>
                </React.Fragment>
            ))

        }
    }
    return (
            <Grid container spacing={3} style={{height: '670px', overflow: 'scroll'}}>
                {jsxElement}
            </Grid>
        );
}

const getImagePlanOperator = (planOperator:string | number | boolean | undefined) => {

    let image;
    switch (planOperator){
        case 'PROJECT':
            image=projection;
            break;
        case 'NEST':
            image=nest;
            break;
        case 'SUM':
            image=sumAggregate;
            break;
        case 'OUTERJOIN':
            image=leftOuterJoin;
            break;
        case 'UNNEST':
            image=unnest;
            break;
        case 'JOIN':
            image=equiJoin;
            break;
        default:
            return;
    }
    return <img src={image} alt={'planOperatorSymbol'} width={30} height={30}/>
}

//todo add to global state
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

const getNestedDepth = (jsonObject: RawNodeDatum, depth = 1,index =0) => {
    let results = 0;
    if(jsonObject.children?.length == 0){
        return depth
    }else{
        results = getNestedDepth(jsonObject.children![index], depth +1, 0)
    };
    return results;

}


// Here we're using `renderCustomNodeElement` to bind event handlers
// to the DOM nodes of our choice.
// In this case, we only want the node to toggle if the *label* is clicked.
// Additionally we've replaced the circle's `onClick` with a custom event,
// which differentiates between branch and leaf nodes.
const renderNodeWithCustomEvents = (diagramElProps: CustomNodeElementProps, index:number) => {
    // @ts-ignore
    const level = parseInt(diagramElProps.nodeDatum.attributes?.level!);
    const levelColor = _colorPicker(index);
    return (
        <g>
            <circle r="15" onClick={diagramElProps.toggleNode} fill={levelColor}/>
            <foreignObject width={70} height={50}>
                {getImagePlanOperator(diagramElProps.nodeDatum.attributes?.planOperator)}
            </foreignObject>
            <text fill="black" strokeWidth="0.5" x="50" y={"0"} fontSize={28} onClick={diagramElProps.toggleNode}>
                {diagramElProps.nodeDatum.attributes?.planOperator}
            </text>
            <text fill="black" strokeWidth="0.5" x="50" y={"15"} fontSize={25} onClick={diagramElProps.toggleNode}>
                {diagramElProps.nodeDatum.name}
            </text>
            {diagramElProps.nodeDatum.attributes?.newLine && (
                <text fill="black" x="50" dy="30" fontSize={20} strokeWidth="0.5">
                    {diagramElProps.nodeDatum.attributes?.newLine}
                </text>
            )}
            {level >= 0 && (
                <text fill="black" x="50" fontSize={18} dy={diagramElProps.nodeDatum.attributes?.newLine?"50":"40"} strokeWidth="1">
                    level: {diagramElProps.nodeDatum.attributes?.level}
                </text>
            )}
        </g>
    );
}

export default PlanResult;
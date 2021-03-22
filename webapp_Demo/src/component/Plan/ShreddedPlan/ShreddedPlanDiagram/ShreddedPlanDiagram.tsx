import React from "react";
import {Grid} from "@material-ui/core";
import Tree from "react-d3-tree";
import {CustomNodeElementProps, RawNodeDatum} from "react-d3-tree/lib/types/common";
import projection from "../../../../static/images/planOperator/Projection.png";
import nest from "../../../../static/images/planOperator/Nest.png";
import sumAggregate from "../../../../static/images/planOperator/Sum_aggregate.png";
import leftOuterJoin from "../../../../static/images/planOperator/LeftOuterJoin.png";
import unnest from "../../../../static/images/planOperator/Unnest.png";
import equiJoin from '../../../../static/images/planOperator/Equijoin.png';

const ShreddedPlanDiagram = () => (
    <Grid item xs={12}>
        <Grid item direction={"row"}>
            <Tree
                data={treeDiagramLvl1}
                orientation={"vertical"}
                pathFunc={"straight"}
                zoom={0.6}
                enableLegacyTransitions
                translate={{x:400, y:20}}
                transitionDuration={1500}
                renderCustomNodeElement={diagramProps => renderNodeWithCustomEvents(diagramProps)}
                zoomable={false}
            />
        </Grid>
        <Grid item direction={"row"}>
            <Tree
                data={treeDiagramLvl2}
                orientation={"vertical"}
                pathFunc={"straight"}
                zoom={0.6}
                enableLegacyTransitions
                translate={{x:400, y:20}}
                transitionDuration={1500}
                renderCustomNodeElement={diagramProps => renderNodeWithCustomEvents(diagramProps)}
                zoomable={false}
            />
        </Grid>
        <Grid item direction={"row"} style={{height:400}}>
            <Tree
                data={treeDiagramData}
                orientation={"vertical"}
                pathFunc={"straight"}
                zoom={0.6}
                enableLegacyTransitions
                translate={{x:400, y:20}}
                transitionDuration={1500}
                renderCustomNodeElement={diagramProps => renderNodeWithCustomEvents(diagramProps)}
                separation={{siblings:1.7}}
                zoomable={false}
            />
        </Grid>
    </Grid>
);

const treeDiagramLvl1:RawNodeDatum = {
    name: '', //'sample,mutations:=Newlabel(sample)',
    attributes: {
        newLine: 'sample,mutations:=Newlabel(sample)',
        level:'1',
        planOperator:'projection'
    },
    children:[
        {
            name: 'MatSamples',
            attributes: {
                level: '1'
            },
        }
    ]
}
const treeDiagramLvl2:RawNodeDatum = {
    name: '', //'mutId, scores:= NewLabel(sample)',
    attributes: {
        newLine: 'mutId, scores:= NewLabel(sample)',
        level:'2',
        planOperator:'projection'
    },
    children:[
        {
            name: 'MatOccurences',
            attributes: {
                level: '2'
            },
        }
    ]
}
const treeDiagramData:RawNodeDatum = {
    name:'', //'label,gene,score',
    attributes:{
        newLine: 'label,gene,score',
        level:'3',
        planOperator:'projection'
    },
    children:[{
        name: 'impact*(cnum+0.01)*sift*poly',
        attributes:{
            //newLine: 'impact*(cnum+0.01)*sift*poly', 
            newLine: 'sample, label, gene',
            level:'3',
            planOperator:'sum-aggregate'
        },
        children:[{
            name:'',
            attributes:{
                newLine: 'sample, gene',
                level:'3',
                planOperator:'equijoin'
            },
            children:[
                {
                    name:'',
                    attributes:{
                        newLine: 'label',
                        level:'3',
                        planOperator:'equijoin'
                    },
                    children:[
                        {
                            name:'LabDomain', //mut', //ations_scores',
                            attributes:{
                                level:'3'
                            }
                        },
                        {
                            name:'MatOccurences', //mutations',
                            attributes:{
                                level:'3'
                            }
                        }
                    ]
                },
                {
                    name:'CopyNumber',
                    attributes:{
                        level:'3'
                    }
                }
            ]
        }]
    }]
}


// Here we're using `renderCustomNodeElement` to bind event handlers
// to the DOM nodes of our choice.
// In this case, we only want the node to toggle if the *label* is clicked.
// Additionally we've replaced the circle's `onClick` with a custom event,
// which differentiates between branch and leaf nodes.
const renderNodeWithCustomEvents = (diagramElProps: CustomNodeElementProps) => {
    const levelColor = _colorPicker(diagramElProps.nodeDatum.attributes?.level!);
    return (
        <g>

            <circle r="15" onClick={diagramElProps.toggleNode} fill={levelColor}/>
            <foreignObject width={70} height={50}>
                {getImagePlanOperator(diagramElProps.nodeDatum.attributes?.planOperator)}
            </foreignObject>
            <text fill="black" strokeWidth="0.5" x="50" y={"20"} fontSize={25} onClick={diagramElProps.toggleNode}>
                {diagramElProps.nodeDatum.name}
            </text>
            {diagramElProps.nodeDatum.attributes?.newLine && (
                <text fill="black" x="50" dy="40" strokeWidth="0.5" fontSize={27}>
                    {diagramElProps.nodeDatum.attributes?.newLine}
                </text>
            )}
            {diagramElProps.nodeDatum.attributes?.level && (
                <text fill="black" x="50" fontSize={18} dy={diagramElProps.nodeDatum.attributes?.newLine?"60":"40"} strokeWidth="1">
                    level: {diagramElProps.nodeDatum.attributes?.level}
                </text>
            )}
        </g>
    );
}

const _colorPicker =(level:String)=>{
    switch (level) {
        case "1":
            return "#8D9E91";
        case "2":
            return "#8B7A8C";
        case "3":
            return "#B382B5";
    }
}

const getImagePlanOperator = (planOperator:String | undefined) => {

    let image;
    switch (planOperator){
        case 'projection':
            image=projection;
            break;
        case 'nest':
            image=nest;
            break;
        case 'sum-aggregate':
            image=sumAggregate;
            break;
        case 'left-outer-join':
            image=leftOuterJoin;
            break;
        case 'unnest':
            image=unnest;
            break;
        case 'equijoin':
            image=equiJoin;
            break;
        default:
            return;
    }
    return <img src={image} alt={'planOperatorSymbol'} width={30} height={30}/>
}

export default ShreddedPlanDiagram;
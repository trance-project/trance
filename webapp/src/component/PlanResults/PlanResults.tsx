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
import ExpandMoreIcon from "@material-ui/icons/ExpandMore";

import S from "../ui/Span/S";
import projection from "../../static/images/planOperator/Projection.png";
import nest from "../../static/images/planOperator/Nest.png";
import sumAggregate from "../../static/images/planOperator/Sum_aggregate.png";
import leftOuterJoin from "../../static/images/planOperator/LeftOuterJoin.png";
import unnest from "../../static/images/planOperator/Unnest.png";
import equiJoin from "../../static/images/planOperator/Equijoin.png";


interface _PlanResultsProps{

}

const PlanResult = (props:_PlanResultsProps) => {
    const classes = planResultsThemeStyle();

    return (
            <Grid container spacing={3} style={{height: '100vh'}}>
                <Grid item xs={12} md={6}>
                    <Grid item direction={"row"}>
                        <Tree
                            data={treeDiagramLvl1}
                            orientation={"vertical"}
                            pathFunc={"straight"}
                            zoom={0.6}
                            enableLegacyTransitions
                            translate={{x:200, y:20}}
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
                            translate={{x:200, y:20}}
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
                            translate={{x:200, y:20}}
                            transitionDuration={1500}
                            renderCustomNodeElement={diagramProps => renderNodeWithCustomEvents(diagramProps)}
                            separation={{siblings:1.7}}
                            zoomable={false}
                        />
                    </Grid>
                </Grid>
                <Grid item xs={12} md={6}>
                    <Accordion className={classes.accordion1lvl} defaultExpanded>
                        <AccordionSummary
                            expandIcon={<ExpandMoreIcon />}
                            aria-controls="panel1a-content"
                            id="panel1a-header"

                        >
                            <Typography className={classes.heading}>{'TopBag <='}</Typography>
                        </AccordionSummary>
                        <AccordionDetails>
                            <Typography className={classes.body} component={"div"}>
                                <Typography>
                                    <S variant={"highlight"}>for</S> s<S variant={"accent"}>F</S> in MATSAMPLE <S variant={"highlight"}>union</S>
                                </Typography>
                                <Typography style={{textIndent: '10px'}}>
                                    {"{("}sample := s<S variant={"accent"}>F</S>.sample,mutations := NewLabel(s<S variant={"accent"}>F</S>.sample){"}"}
                                </Typography>
                            </Typography>
                        </AccordionDetails>
                    </Accordion>
                    <Accordion className={classes.accordion2lvl} defaultExpanded>
                        <AccordionSummary
                            expandIcon={<ExpandMoreIcon />}
                            aria-controls="panel3a-content"
                            id="panel3a-header"
                        >
                            <Typography className={classes.heading}>MATDICT<S variant={"shrink"}>mutations</S>{'<='}</Typography>
                        </AccordionSummary>
                        <AccordionDetails>
                            <Typography className={classes.body} component={'div'}>
                                <Typography variant={'body1'}><S variant={"highlight"}>for</S> l in LABDOMAIN<S variant={"shrink"}>mutations</S> <S variant={"highlight"}>union</S></Typography>
                                <Typography style={{textIndent: '10px'}} variant={'body1'}>groupBy<S variant={"shrink"}>NewLabel(sample)</S>(</Typography>
                                <Typography style={{textIndent: '30px'}} ><S variant={"highlight"}>for</S> o<S variant={"accent"}>F</S> <S variant={"highlight"}>in</S> MATOCCURRENCES</Typography>
                                <Typography style={{textIndent: '40px'}}><S variant={"highlight"}>union</S>{'{'}(sample:=o<S variant={"accent"}>F</S>.sample,</Typography>
                                <Typography style={{textIndent: '50px'}}><S variant={"highlight"}>union</S> mutId := o<S variant={"accent"}>F</S>.mutId, score:= NewLabel(o<S variant={"accent"}>F</S>.sample,o<S variant={"accent"}>F</S>.mutations)){'})'}</Typography>
                                <Accordion className={classes.accordionWhite}>
                                    <AccordionSummary
                                        expandIcon={<ExpandMoreIcon />}
                                        aria-controls="panel2a-content"
                                        id="panel2a-header"
                                    >
                                        <Typography className={classes.heading}>LABDOMAIN<S variant={"shrink"}>mutations_scores</S> {'<='}</Typography>
                                    </AccordionSummary>
                                    <AccordionDetails>
                                        <Typography>
                                            dedup(<S variant={"highlight"}>for</S> l in MATDICT<S variant={"shrink"}>mutations</S> <S variant={"highlight"}>union</S> {"{(label := l.scores)})"}
                                        </Typography>
                                    </AccordionDetails>
                                </Accordion>
                            </Typography>
                        </AccordionDetails>
                    </Accordion>
                    <Accordion className={classes.accordion3lvl} defaultExpanded>
                        <AccordionSummary
                            expandIcon={<ExpandMoreIcon />}
                            aria-controls="panel4a-content"
                            id="panel4a-header"
                        >
                            <Typography className={classes.heading}>MatDICT<S variant={"shrink"}>mutation.scores</S> {'<='}</Typography>
                        </AccordionSummary>
                        <AccordionDetails>
                            <Typography className={classes.body} component={'div'}>
                                <Typography><S variant={"highlight"}>for</S> l <S variant={"highlight"}>in</S> LABDOMAIN<S variant={"shrink"}>mutations.scores</S> <S variant={"highlight"}>union</S></Typography>
                                <Typography style={{textIndent: '10px'}}>{'{(label:= l.label,'}</Typography>
                                <Typography style={{textIndent: '20px'}}>value:=<S variant={"highlight"}>match</S> l.label = Newlabel(o<S variant={"accent"}>F</S>.sample, o<S variant={"accent"}>F</S>.mutations) <S variant={"highlight"}>then</S></Typography>
                                <Typography style={{textIndent: '30px'}}>{'sumBy^{score}_{gene}('}</Typography>
                                <Typography style={{textIndent: '40px'}}><S variant={"highlight"}>for</S> t<S variant={"accent"}>F</S> in MatLookup(MATOCCURRENCES<S variant={"shrink"}>mutations</S>,o<S variant={"accent"}>F</S>.mutations)</Typography>
                                <Typography style={{textIndent: '50px'}}><S variant={"highlight"}>for</S> c<S variant={"accent"}>F</S> <S variant={"highlight"}>in</S> CopyNumber<S variant={"accent"}>F</S> <S variant={"highlight"}>union</S></Typography>
                                <Typography style={{textIndent: '60px'}}><S variant={"highlight"}>if</S>(t<S variant={"accent"}>F</S>.gene == c<S variant={"accent"}>F</S>.gene && o<S variant={"accent"}>F</S>.sample === c<S variant={"accent"}>F</S>.sample) <S variant={"highlight"}>then</S></Typography>
                                <Typography style={{textIndent: '70px'}}>{'{'}(gene := t<S variant={"accent"}>F</S>.gene,score := t<S variant={"accent"}>F</S>.impact * (c<S variant={"accent"}>F</S>.cnum + 0.01) * t<S variant={"accent"}>F</S>.sift * t<S variant={"accent"}>F</S>.poly{'})})'}</Typography>
                            </Typography>
                        </AccordionDetails>
                    </Accordion>
                </Grid>
            </Grid>
        );
}

const treeDiagramLvl1:RawNodeDatum = {
    name: 'sample,mutations:=Newlabel(sample)',
    attributes: {
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
    name: 'mutId, scores:= NewLabel(sample)',
    attributes: {
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
    name:'label,gene,score',
    attributes:{
        level:'3',
        planOperator:'projection'
    },
    children:[{
        name:'impact*(cnum+0.01)*sift*poly',
        attributes:{
            newLine:'sample, label, gene',
            level:'3',
            planOperator:'sum-aggregate'
        },
        children:[{
            name:'sample,gene',
            attributes:{
                level:'3',
                planOperator:'equijoin'
            },
            children:[
                {
                    name:'label',
                    attributes:{
                        level:'3',
                        planOperator:'equijoin'
                    },
                    children:[
                        {
                            name:'LabDomainmutations_scores',
                            attributes:{
                                level:'3'
                            }
                        },
                        {
                            name:'MatOccurencesmutations',
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
            <text fill="black" strokeWidth="0.5" x="50" y={"20"} onClick={diagramElProps.toggleNode}>
                {diagramElProps.nodeDatum.name}
            </text>
            {diagramElProps.nodeDatum.attributes?.newLine && (
                <text fill="black" x="50" dy="40" strokeWidth="0.5">
                    {diagramElProps.nodeDatum.attributes?.newLine}
                </text>
            )}
            {diagramElProps.nodeDatum.attributes?.level && (
                <text fill="black" x="50" dy={diagramElProps.nodeDatum.attributes?.newLine?"60":"40"} strokeWidth="1">
                    level: {diagramElProps.nodeDatum.attributes?.level}
                </text>
            )}
        </g>
    );
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
    return <img src={image} alt={'planOperatorSymbol'} width={20} height={20}/>
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

export default PlanResult;
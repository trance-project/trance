import React from "react";
import CloseIcon from '@material-ui/icons/Close';
import Button from '@material-ui/core/Button';

import {planResultsThemeStyle} from './planResultsThemeStyle';
import {TransitionProps} from "@material-ui/core/transitions";
import {
    Accordion,
    AccordionDetails,
    AccordionSummary,
    AppBar,
    Dialog,
    Grid,
    IconButton,
    Slide,
    Typography
} from "@material-ui/core";

import Toolbar from '@material-ui/core/Toolbar';
import {CustomNodeElementProps, RawNodeDatum} from "react-d3-tree/lib/types/common";
import Tree from "react-d3-tree";
import ExpandMoreIcon from "@material-ui/icons/ExpandMore";

import S from "../ui/Span/S";

const Transition = React.forwardRef((props:TransitionProps & {children?: React.ReactElement}, ref:React.Ref<unknown>)=>{
    return <Slide direction={"up"} ref={ref} {...props}/>;
});

interface _PlanResultsProps{
    open: boolean;
    close: () => void;
    successful: boolean
}

const PlanResult = (props:_PlanResultsProps) => {
    const classes = planResultsThemeStyle();

    return (
        <Dialog open={props.open} fullScreen TransitionComponent={Transition}>
            <AppBar className={classes.appBar}>
                <Toolbar>
                    <IconButton edge={'start'} color={'inherit'} onClick={props.close} aria-label={"close"}>
                        <CloseIcon />
                    </IconButton>
                    <Typography variant={"h6"} className={classes.title}>
                        Shredded Plan
                    </Typography>
                    <Button autoFocus color={"inherit"} onClick={props.close}>
                        Download
                    </Button>
                </Toolbar>
            </AppBar>
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
        </Dialog>
    );
}

const treeDiagramLvl1:RawNodeDatum = {
    name: 'sample,mutations:=Newlabel(sample)',
    attributes: {
        level:'1'
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
        level:'2'
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
                    name:'πlabel,gene,score',
                    attributes:{
                        level:'3'
                    },
                    children:[{
                        name:'impact*(cnum+0.01)*sift*poly',
                        attributes:{
                            newLine:'sample, label, gene',
                            level:'3'
                        },
                        children:[{
                            name:'sample,gene',
                            attributes:{
                                level:'3'
                            },
                            children:[
                                {
                                    name:'label',
                                    attributes:{
                                        level:'3'
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
            <text fill="black" strokeWidth="0.5" x="20" onClick={diagramElProps.toggleNode}>
                {diagramElProps.nodeDatum.name}
            </text>
            {diagramElProps.nodeDatum.attributes?.newLine && (
                <text fill="black" x="20" dy="20" strokeWidth="0.5">
                    {diagramElProps.nodeDatum.attributes?.newLine}
                </text>
            )}
            {diagramElProps.nodeDatum.attributes?.level && (
                <text fill="black" x="20" dy={diagramElProps.nodeDatum.attributes?.newLine?"40":"20"} strokeWidth="1">
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

export default PlanResult;
import React from "react";

import materializationThemeStyle from './materializationThemeStyle'
import {Accordion, AccordionDetails, AccordionSummary, Typography} from "@material-ui/core";
import ExpandMoreIcon from "@material-ui/icons/ExpandMore";
import S from "../../../../ui/Span/S";

const Materializationlvl2 = () => {
    const classes = materializationThemeStyle();

    return (
        <React.Fragment>
            <Accordion className={classes.accordion2lvl} expanded>
                <Accordion className={classes.accordion1lvl} >
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
                                <S variant={"highlight"}>for</S> s<S variant={"accent"}>F</S> <S variant={"highlight"}>in</S> MATSAMPLE <S variant={"highlight"}>union</S>
                            </Typography>
                            <Typography style={{textIndent: '10px'}}>
                                {"{("}sample := s<S variant={"accent"}>F</S>.sample,mutations := NewLabel(s<S variant={"accent"}>F</S>.sample){"}"}
                            </Typography>
                            <Accordion className={classes.accordionWhite}>
                                <AccordionSummary
                                    expandIcon={<ExpandMoreIcon />}
                                    aria-controls="panel2a-content"
                                    id="panel2a-header"
                                >
                                    <Typography className={classes.heading}>LABELDOMAIN<S variant={"shrink"}>mutations</S> {'<='}</Typography>
                                </AccordionSummary>
                                <AccordionDetails>
                                    <Typography>
                                        dedup(<S variant={"highlight"}>for</S> l <S variant={"highlight"}>in</S> TOPBAG <S variant={"highlight"}>union</S> {"{(label := l.mutations)})"}
                                    </Typography>
                                </AccordionDetails>
                            </Accordion>
                        </Typography>
                    </AccordionDetails>
                </Accordion>
                <AccordionSummary
                    expandIcon={<ExpandMoreIcon />}
                    aria-controls="panel3a-content"
                    id="panel3a-header"
                >
                    <Typography className={classes.heading}>MATDICT<S variant={"shrink"}>mutations</S>{'<='}</Typography>
                </AccordionSummary>
                <AccordionDetails>
                    <Typography className={classes.body} component={'div'}>
                        <Typography variant={'body1'}><S variant={"highlight"}>for</S> l <S variant={"highlight"}>in</S> LABELDOMAIN<S variant={"shrink"}>mutations</S> <S variant={"highlight"}>union</S></Typography>
                        <Typography style={{textIndent: '10px'}} variant={'body1'}>{'{(label:= l.label,'}</Typography>
                        <Typography style={{textIndent: '20px'}} variant={'body1'}>value:=<S variant={"highlight"}>match</S> l.label = Newlabel(s<S variant={"accent"}>F</S>.sample) <S variant={"highlight"}>then</S></Typography>
                        <Typography style={{textIndent: '30px'}} ><S variant={"highlight"}>for</S> o<S variant={"accent"}>F</S> <S variant={"highlight"}>in</S> MATOCCURRENCES</Typography>
                        <Typography style={{textIndent: '40px'}}><S variant={"highlight"}>if</S>(o<S variant={"accent"}>F</S>.sample === s<S variant={"accent"}>F</S>.sample) <S variant={"highlight"}>then</S></Typography>
                        <Typography style={{textIndent: '50px'}}><S variant={"highlight"}>union</S> {'{'}(mutId := o<S variant={"accent"}>F</S>.mutId, score:= NewLabel(o<S variant={"accent"}>F</S>.sample,o<S variant={"accent"}>F</S>.mutations)){'}'}</Typography>
                        <Accordion className={classes.accordionWhite}>
                            <AccordionSummary
                                expandIcon={<ExpandMoreIcon />}
                                aria-controls="panel2a-content"
                                id="panel2a-header"
                            >
                                <Typography className={classes.heading}>LABELDOMAIN<S variant={"shrink"}>mutations_scores</S> {'<='}</Typography>
                            </AccordionSummary>
                            <AccordionDetails>
                                <Typography>
                                    dedup(<S variant={"highlight"}>for</S> l <S variant={"highlight"}>in</S> MATDICT<S variant={"shrink"}>mutations</S> <S variant={"highlight"}>union</S> {"{(label := l.scores)})"}
                                </Typography>
                            </AccordionDetails>
                        </Accordion>
                    </Typography>
                </AccordionDetails>
            </Accordion>
        </React.Fragment>
    );
}

export default Materializationlvl2
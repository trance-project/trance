import React from "react";

import materializationThemeStyle from './materializationThemeStyle'
import {Accordion, AccordionDetails, AccordionSummary, Typography} from "@material-ui/core";
import ExpandMoreIcon from "@material-ui/icons/ExpandMore";
import S from "../../../../ui/Span/S";

const Materializationlvl3 = () => {
    const classes = materializationThemeStyle();

    return (
        <React.Fragment>
            <Accordion className={classes.accordion3lvl} expanded>
                <Accordion className={classes.accordion2lvl} >
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
                <AccordionSummary
                    expandIcon={<ExpandMoreIcon />}
                    aria-controls="panel4a-content"
                    id="panel4a-header"
                >
                    <Typography className={classes.heading}>MatDICT<S variant={"shrink"}>mutation.scores</S> {'<='}</Typography>
                </AccordionSummary>
                <AccordionDetails>
                    <Typography className={classes.body} component={'div'}>
                        <Typography><S variant={"highlight"}>for</S> l <S variant={"highlight"}>in</S> LABELDOMAIN<S variant={"shrink"}>mutations.scores</S> <S variant={"highlight"}>union</S></Typography>
                        <Typography style={{textIndent: '10px'}}>{'{(label:= l.label,'}</Typography>
                        <Typography style={{textIndent: '20px'}}>value:=<S variant={"highlight"}>match</S> l.label = Newlabel(o<S variant={"accent"}>F</S>.sample, o<S variant={"accent"}>F</S>.mutations) <S variant={"highlight"}>then</S></Typography>
                        <Typography style={{textIndent: '30px'}}>{'sumBy^{score}_{gene}('}</Typography>
                        <Typography style={{textIndent: '40px'}}><S variant={"highlight"}>for</S> t<S variant={"accent"}>F</S> in MatLookup(MATOCCURRENCES<S variant={"shrink"}>mutations</S>,o<S variant={"accent"}>F</S>.mutations)</Typography>
                        <Typography style={{textIndent: '50px'}}><S variant={"highlight"}>for</S> c<S variant={"accent"}>F</S> <S variant={"highlight"}>in</S> CopyNumber<S variant={"accent"}>F</S> <S variant={"highlight"}>union</S></Typography>
                        <Typography style={{textIndent: '60px'}}><S variant={"highlight"}>if</S>(t<S variant={"accent"}>F</S>.gene == c<S variant={"accent"}>f</S>.gene && o<S variant={"accent"}>F</S>.sample === c<S variant={"accent"}>F</S>.sample) <S variant={"highlight"}>then</S></Typography>
                        <Typography style={{textIndent: '70px'}}>{'{'}(gene := t<S variant={"accent"}>F</S>.gene,score := t<S variant={"accent"}>F</S>.impact * (c<S variant={"accent"}>F</S>.cnum + 0.01) * t<S variant={"accent"}>F</S>.sift * t<S variant={"accent"}>F</S>.poly{'})})'}</Typography>
                    </Typography>
                </AccordionDetails>
            </Accordion>
        </React.Fragment>
    );
}

export default Materializationlvl3
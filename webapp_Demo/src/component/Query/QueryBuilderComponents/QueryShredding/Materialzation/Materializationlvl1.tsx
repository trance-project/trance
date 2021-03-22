import React from "react";

import materializationThemeStyle from './materializationThemeStyle'
import {Accordion, AccordionDetails, AccordionSummary, Typography} from "@material-ui/core";
import ExpandMoreIcon from "@material-ui/icons/ExpandMore";
import S from "../../../../ui/Span/S";

const Materializationlvl1 = () => {
 const classes = materializationThemeStyle();

 return (
     <Accordion className={classes.accordion1lvl} defaultExpanded >
         <AccordionSummary
             expandIcon={<ExpandMoreIcon />}
             aria-controls="panel1a-content"
             id="panel1a-header"

         >
             <Typography className={classes.heading}>{'TopBag <='}</Typography>
         </AccordionSummary>
         <AccordionDetails>
             <Typography className={classes.body} component={"div"}>
                 <Typography variant={"body1"}>
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
                         <Typography className={classes.heading}>LABDOMAIN<S variant={"shrink"}>mutations</S> {'<='}</Typography>
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
 );
}

export default Materializationlvl1
import React, {Children,useState} from "react";

import TabPanel from "./TabPanel/TabPanel";
import {customsTabsThemeStyle} from './CustomsTabsThemeStyle'
import {AppBar, useTheme} from "@material-ui/core";
import Tabs from "@material-ui/core/Tabs";
import Tab from '@material-ui/core/Tab';
import SwipeableViews from 'react-swipeable-views';
import {customTabElement} from '../../../Interface/Public_Interfaces';

const a11yProps = (index: any) => ({
    id:`full-width-tab-${index}`,
    'aria-controls': `full-width-tabpanel-${index}`
});

interface _CustomTabs {
  tabsElement : customTabElement[],
  scrollable?: boolean;
}

const CustomTabs = (props:_CustomTabs) => {
   const classes = customsTabsThemeStyle();
   const theme = useTheme();
   const [valueState, setValueState] = useState(0);
   let tabsAppbarLabel:React.ReactNode[] = [];
   let tabsPanel:React.ReactNode[] = [];

   const handleTabChange = (event: React.ChangeEvent<{}>, newValue: number) => {
       setValueState(newValue);
   }

   const handleChangeIndex = (index: number) => {
       setValueState(index)
   }

   props.tabsElement.forEach((element, i) => {
       tabsAppbarLabel.push(<Tab key={i} label={element.tabLabel} {...a11yProps(i)}  disabled={element.disable}/>);
       tabsPanel.push(
           <TabPanel key={i} index={valueState} value={i} dir={theme.direction}>
               {element.jsxElement}
           </TabPanel>
       );
   })

   return (
       <div className={classes.root}>
           <AppBar position={'static'} color={"default"}>
               <Tabs
                   value={valueState}
                   onChange={handleTabChange}
                   indicatorColor={"primary"}
                   textColor={"primary"}
                   variant={props.scrollable?"scrollable":"standard"}
                   aria-label={'full width tabs query view'}

                   >
                   {tabsAppbarLabel}
               </Tabs>
           </AppBar>
           <SwipeableViews
               axis={theme.direction === 'rtl' ? 'x-reverse' : 'x'}
               index={valueState}
               onChangeIndex={handleChangeIndex}>
               {tabsPanel}
           </SwipeableViews>
       </div>
   )
};

export default CustomTabs;
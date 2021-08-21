import React from "react";
import {createStyles, fade, Theme, withStyles} from '@material-ui/core/styles';
import TreeItem, { TreeItemProps } from '@material-ui/lab/TreeItem';
import { TransitionProps } from '@material-ui/core/transitions';
import { useSpring, animated } from 'react-spring/web.cjs'; // web.cjs is required for IE 11 support
import Collapse from '@material-ui/core/Collapse';


function TransitionComponent(props: TransitionProps) {
    const style = useSpring({
        from: { opacity: 0, transform: 'translate3d(20px,0,0)' },
        to: { opacity: props.in ? 1 : 0, transform: `translate3d(${props.in ? 0 : 20}px,0,0)` },
    });

    return (
        <animated.div style={style}>
            <Collapse {...props} />
        </animated.div>
    );
}

const StyledTreeItem = withStyles((theme: Theme) =>
    createStyles({
        iconContainer: {
            '& .close': {
                opacity: 0.3,
            },
        },
        group: {
            marginLeft: 7,
            paddingLeft: 18,
            borderLeft: `1px dashed ${fade(theme.palette.text.primary, 0.4)}`,
        },
    }),
)((props: TreeItemProps) => <TreeItem {...props} TransitionComponent={TransitionComponent} />);

export default StyledTreeItem;

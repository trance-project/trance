import React, {useEffect, useState, useRef} from "react";
import * as d3 from "d3";
import {TableGraphMetaInfo} from '../../../../utils/Public_Interfaces';

const width = 950;
const margin = { top: 20, right: 5, bottom: 20, left: 35 };





type barPlots = {
    x: number,
    y: number,
    height: number,
    fill: string
}

interface _SimpleBarGraphProps {
    height: number;
    data: TableGraphMetaInfo[]  | undefined;
}
const SimpleBarGraph = (props: _SimpleBarGraphProps) => {

    const [bars, setBars] = useState<barPlots[]>([]);

    // @ts-ignore
    const xAxis = d3.axisBottom();
    const xAxisRef = useRef(null);

    // @ts-ignore
    const yAxis = d3.axisLeft();
    const yAxisRef = useRef(null);

    const barsRef = useRef(null);

    useEffect(() =>{

         const processData = async () =>{
             if(props.data) {
                 // 1. map index = x-position
                 // get min and max of id
                 const xExtent = d3.extent(props.data, d => d.id)
                 const xScale = d3.scaleLinear()
                     // @ts-ignore
                     .domain(xExtent)
                     .range([margin.left, width - margin.right]);

                 //2. map count to y-position
                 // get min and max of count
                 const yExtent = d3.extent(props.data, d => d.count);
                 const yScale = d3.scaleLinear()
                     // @ts-ignore
                     .domain(yExtent)
                     .range([props.height - margin.bottom, margin.top])

                 const colorScale = d3.scaleSequential()
                     // @ts-ignore
                     .domain(yExtent)
                     .interpolator(d3.interpolateRdYlBu);

                 // array of objects: x, y, height
                 const bars = await props.data.map(d => {
                     return {
                         x: xScale(d.id),
                         y: yScale(d.count),
                         height: yScale(0) - yScale(d.count),
                         fill: colorScale(d.count)
                     };
                 });

                 // @ts-ignore
                 xAxis.scale(xScale);
                 // @ts-ignore
                 d3.select(xAxisRef.current).call(xAxis);
                 // @ts-ignore
                 yAxis.scale(yScale)
                 // @ts-ignore
                 d3.select(yAxisRef.current).call(yAxis);

                 setBars(bars)

                 d3.select(barsRef.current)
                     .selectAll('rect')
                     .data(bars)
                     // .transition()
                     .attr('y', d=>d.y)
                     .attr('x', d=>d.x)
                     .attr('height', d=>d.height)
                     .attr('fill', d => d.fill);
             }
         }

         processData();

    }, [props.data, props.height])

    console.log('[bars]', bars);

        return (
                <svg width={width} height={props.height}>
                    <g ref={barsRef}>
                        {bars.map((d,i) => (
                            <rect key={i} width={2} />
                        ))}
                    </g>
                    <g ref={xAxisRef} transform={`translate(0, ${props.height - margin.bottom})`}/>
                    <g ref={yAxisRef} transform={`translate(${margin.left}, 0)`}/>
                </svg>
        );
}

export default SimpleBarGraph
import React, {useState,useEffect, useRef}  from 'react';
import Typography from "@material-ui/core/Typography";
import * as d3 from 'd3';

import {RunTimeMetrics} from '../../../../utils/Public_Interfaces';



interface _SimpleAreaGraphVShreddedProps{
    data: RunTimeMetrics[];
}

const SimpleAreaGraphVShredded = (props:_SimpleAreaGraphVShreddedProps) => {
    const margin = { top: 20, right: 5, bottom: 20, left: 50 };
    const width = 1050;
    const height= 250;

    const [lineChartPath, setLineChartPath] = useState<{path: any, fill:string}[]>()
    const [currentZoomState, setCurrentZoomState] = useState()

    // @ts-ignore
    const yAxis = d3.axisLeft();

    const yAxisRef = useRef(null);
    const brushRef=useRef(null);
    const svgRef=useRef(null);
    const linesRef=useRef(null);

    useEffect(()=>{
        const svg = d3.select(svgRef.current);
        const extentX = d3.extent(props.data, d=>d.pid);

        const xScale = d3.scaleTime()
            // @ts-ignore
            .domain(extentX).range([margin.left,width - margin.right])

        const highMax = d3.max(props.data, d => d.stand_write_size)
        const lowMin = d3.min(props.data, d => d.stand_write_size)


        const yScale = d3.scaleLinear()
            // @ts-ignore
            .domain([lowMin, highMax])
            .range([height-margin.bottom, margin.top])

        if (currentZoomState) {
            // @ts-ignore
            const newXScale = currentZoomState!.rescaleX(xScale);

            // @ts-ignore
            const newYScale = currentZoomState!.rescaleY(yScale);
            // xScale.domain(newXScale.domain());
            yScale.domain(newYScale.domain());
        }

        // @ts-ignore
        const line = d3.line().x(d => xScale(d.pid))


        const linePath = [
            // @ts-ignore
            {path: line.y(d=>yScale(d.shred_write_size))(props.data), fill:"red"},
            // @ts-ignore
            {path: line.y(d=>yScale(d.stand_write_size))(props.data), fill:"blue"},
        ]

        if(yAxisRef.current){
            // @ts-ignore
            yAxis.scale(yScale)
            // @ts-ignore
            d3.select(yAxisRef.current).call(yAxis);
        }

        // zoom
        const zoomBehavior = d3.zoom()
            .scaleExtent([1, 1000])
            .translateExtent([
                [margin.left, margin.top],
                [width - margin.right, height-margin.bottom]
            ])
            .on("zoom", (event) => {
                const zoomState = event.transform;
                setCurrentZoomState(zoomState);
            });

        // @ts-ignore
        svg.call(zoomBehavior)

        // const brushEffect = d3.brush()
        //     .extent([
        //         [margin.left, margin.top],
        //         [width - margin.right, height-margin.bottom]
        //     ])
        //     .on("end", (e)=>{
        //         const s= e.selection;
        //         console.log('[BrushEffect]', s);
        //         if(s){
        //             xScale.domain([s[0][0], s[1][0]].map(xScale.invert, xScale));
        //             yScale.domain([s[1][1], s[0][1]].map(yScale.invert, yScale));
        //             // d3.select(brushRef.current).call()
        //         }
        //         zoom()
        //     });
        //
        // const zoom = () =>
        // {
        //     d3.zoom()
        //         .scaleExtent([1, 32])
        //         .extent([[margin.left, 0], [width - margin.right, height]])
        //         .translateExtent([[margin.left, -Infinity], [width - margin.right, Infinity]])
        //         .on("zoom", zoomed)
        // }


        // @ts-ignore
        // d3.select(brushRef.current).call(brushEffect);




        // @ts-ignore
        setLineChartPath(linePath);

    }, [currentZoomState]);

    /**
     * Function that handles the brushEffect + Zoom
     */


    const brushEnd = () => {
        // console.log("[brush Select]", d3.)
    }

    console.log('[currentZoomState]', currentZoomState)
    return (
      <div style={{ width: '100%' }}>
        <Typography variant={'h6'}>Ordered by size</Typography>
          <svg height={height - margin.bottom} width={width} ref={svgRef}>
              <g ref={linesRef}>
                  {lineChartPath?.map(d => (
                          <path d={d.path} fill={"none"} strokeWidth={2} stroke={d.fill}/>
                      )
                  )}
              </g>
            <g ref={yAxisRef} transform={`translate(${margin.left}, 0)`}/>
              <g ref={brushRef}/>
          </svg>
      </div>
    );
}

export default SimpleAreaGraphVShredded
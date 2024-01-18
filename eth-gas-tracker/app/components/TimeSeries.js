// TimeSeriesChart.js
"use client"
import React, { useState, useEffect } from 'react';
import { Line } from 'react-chartjs-2';
import 'chart.js/auto';
import styles from '../styles/TimeSeries.module.css';
import {Chart, LineSeries, TimeScale, PriceScale} from "lightweight-charts-react-wrapper";

function weiToEther(weiValue) {
    // First, round to the nearest wei
    weiValue = Math.round(weiValue);
    // Convert to string and pad with zeros if necessary
    let weiString = weiValue.toString().padStart(18, '0');

    // Insert decimal point
    let etherValue = weiString.slice(0, -18) + "." + weiString.slice(-18);

    // Remove any trailing zeros and the decimal point if not needed
    etherValue = etherValue.replace(/\.?0+$/, '');

    return parseFloat(etherValue);
}

const TimeSeriesChart = () => {
    const [chartData, setChartData] = useState([]);
        useEffect(() => {
        fetch('http://localhost:3000/api/getHistory').then(res => {
            if (!res.ok) {
                throw Error("Could not fetch data for that resource");
            }
            return res.json()
        }).then(data => {
            console.log(data);
            return data;
        }).then(data => {
            let rows = [];
            for (var i = 0; i < data.labels.length; i++) {
                // time gets converted to unix timestamp
                let time = new Date(data.labels[i]).getTime() / 1000;
                let row = {time: time, value: weiToEther(parseFloat(data.values[i]))* 2000};
                rows.push(row);
            }
            console.log(rows);
            setChartData(rows);
        }).catch(err => {
            console.log(err.message);
        })
    }, []);



    return (
        <div className={styles.graph}>
            {chartData.length > 0 &&
                <Chart width={800} height={600}>
                    <LineSeries data={chartData}/>
                    <TimeScale timeVisible={true} secondsVisible={true} />
                </Chart>
            }
        </div>

    );
};

export default TimeSeriesChart;


/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


jQuery.when(
    jQuery.getScript('https://code.highcharts.com/stock/highstock.js'),
    jQuery.getScript('https://code.highcharts.com/stock/modules/exporting.js'),


    //jQuery("#tschart_${paragraphId}").ready,
    jQuery('#highstock_' + $z.id).ready,
    jQuery.Deferred(function( deferred ){
        jQuery( deferred.resolve );
    })
).done(function(){
    var data = [
[1251072000000,24.30,24.39,24.04,24.15,101732176],
[1251158400000,24.21,24.42,24.16,24.20,81088343],
[1251244800000,24.13,24.22,23.82,23.92,75999469],
[1251331200000,24.11,24.22,23.55,24.21,112294826],
[1251417600000,24.61,24.64,24.08,24.29,113464428],
[1251676800000,24.02,24.12,23.79,24.03,77885087],
[1251763200000,24.00,24.29,23.56,23.61,117256335],
[1251849600000,23.52,23.94,23.44,23.60,91100107],
[1251936000000,23.78,23.87,23.57,23.79,73525711],
[1252022400000,23.90,24.39,23.87,24.33,93656654],
[1252368000000,24.71,24.73,24.57,24.70,78761627],
[1252454400000,24.68,24.92,24.24,24.45,202822886],
[1252540800000,24.58,24.75,24.40,24.65,122783346],
[1252627200000,24.70,24.74,24.41,24.59,87240188],
[1252886400000,24.40,24.84,24.32,24.82,80502611],
[1252972800000,24.86,25.09,24.80,25.02,106617553],
[1253059200000,25.43,26.11,25.41,25.98,188505499],
[1253145600000,26.00,26.68,26.00,26.36,202642664],
[1253232000000,26.55,26.65,26.39,26.43,150436860],
[1253491200000,26.33,26.45,25.95,26.29,109428907],
[1253577600000,26.46,26.48,26.12,26.35,89240991],
[1253664000000,26.49,26.99,26.43,26.50,148509963],
[1253750400000,26.74,26.81,26.11,26.26,137719491],
[1253836800000,26.00,26.50,25.92,26.05,111371862],
[1254096000000,26.27,26.67,26.19,26.59,84411999],
[1254182400000,26.68,26.77,26.33,26.48,86346379],
[1254268800000,26.59,26.64,26.09,26.48,134895635],
[1254355200000,26.48,26.60,25.81,25.84,131177739],
[1254441600000,25.92,26.56,25.91,26.41,138358668],
[1254700800000,26.60,26.69,26.32,26.57,105783062],
[1254787200000,26.82,27.14,26.76,27.14,151270875],
[1254873600000,27.11,27.22,27.00,27.18,116416909],
[1254960000000,27.24,27.35,26.98,27.04,109552296],
[1255046400000,27.00,27.24,26.95,27.21,73348345],
[1255305600000,27.29,27.36,27.09,27.26,72037756],
[1255392000000,27.23,27.31,27.10,27.15,87004582],
[1255478400000,27.46,27.47,27.18,27.33,93942639],
[1255564800000,27.09,27.27,27.08,27.22,93388722],
[1255651200000,27.05,27.19,26.83,26.86,107856189],
[1255910400000,26.84,27.14,26.51,27.12,235557336],
[1255996800000,28.66,28.82,28.26,28.39,285259814],
[1256083200000,28.50,29.82,28.46,29.27,298431525],
[1256169600000,29.24,29.69,28.93,29.31,197847825],
[1256256000000,29.39,29.40,29.03,29.13,105196434],
[1256515200000,29.10,29.54,28.59,28.93,121084383],
[1256601600000,28.81,28.97,28.06,28.20,189137473]];

    // split the data set into ohlc and volume
    var ohlc = [],
        volume = [],
        dataLength = data.length,
        // set the allowed units for data grouping
        groupingUnits = [[
            'week',                         // unit name
            [1]                             // allowed multiples
        ], [
            'month',
            [1, 2, 3, 4, 6]
        ]],

        i = 0;

    for (i; i < dataLength; i += 1) {
        ohlc.push([
            data[i][0], // the date
            data[i][1], // open
            data[i][2], // high
            data[i][3], // low
            data[i][4] // close
        ]);

        volume.push([
            data[i][0], // the date
            data[i][5] // the volume
        ]);
    }

    //jQuery("#tschart_${paragraphId}").highcharts({
    jQuery('#highstock_' + $z.id).highcharts({

        rangeSelector: {
            selected: 1
        },

        title: {
            text: 'AAPL Historical'
        },

        yAxis: [{
            labels: {
                align: 'right',
                x: -3
            },
            title: {
                text: 'OHLC'
            },
            height: '60%',
            lineWidth: 2
        }, {
            labels: {
                align: 'right',
                x: -3
            },
            title: {
                text: 'Volume'
            },
            top: '65%',
            height: '35%',
            offset: 0,
            lineWidth: 2
        }],


        series: [{
            type: 'candlestick',
            name: 'AAPL',
            data: ohlc,
            dataGrouping: {
                units: groupingUnits
            }
        }, {
            type: 'column',
            name: 'Volume',
            data: volume,
            yAxis: 1,
            dataGrouping: {
                units: groupingUnits
            }
        }]
    });
})
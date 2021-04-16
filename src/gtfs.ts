import * as util from 'util';
import * as duckdb from 'duckdb';
import * as express from 'express';
import * as moment from 'moment';
import query from "./query";
import {hmsToMoment} from "./util";


//
// import { Connection, DuckDB } from "node-duckdb";
//
async function run() {
    console.log(__dirname);

    let gtfsdb = new duckdb.Database(':memory:'); // or a file name for a persistent DB

    console.time('Import');
    console.log('[Import]', 'Inserting stop areas');
    await query(`create table stop_areas as SELECT * FROM read_csv_auto('${__dirname + '/tmp'}/stop-areas.csv', HEADER=TRUE, SAMPLE_SIZE=20000);`, gtfsdb);

    console.log('[Import]', 'Inserting CHB stops');
    await query(`create table chb_stops as SELECT * FROM read_csv_auto('${__dirname + '/tmp'}/chb-stops.csv', HEADER=TRUE, SAMPLE_SIZE=20000);`, gtfsdb);

    console.log('[Import]', 'Inserting agency');
    await query(`create table agency as SELECT * FROM read_csv_auto('${__dirname + '/tmp'}/agency.txt', HEADER=TRUE, SAMPLE_SIZE=20000);`, gtfsdb);

    console.log('[Import]', 'Inserting trips');
    await query(`create table trips as SELECT * FROM read_csv_auto('${__dirname + '/tmp'}/trips.txt', HEADER=TRUE, SAMPLE_SIZE=20000);`, gtfsdb);

    console.log('[Import]', 'Inserting routes');
    await query(`create table routes as SELECT * FROM read_csv_auto('${__dirname + '/tmp'}/routes.txt', HEADER=TRUE, SAMPLE_SIZE=20000);`, gtfsdb);

    console.log('[Import]', 'Creating stops table');
    await query(`create table stops
                 (
                     stop_id             VARCHAR(16) NOT NULL,
                     stop_code           VARCHAR(20),
                     stop_name           VARCHAR(80) NOT NULL,
                     stop_lat            VARCHAR(20) NOT NULL,
                     stop_lon            VARCHAR(20) NOT NULL,
                     location_type       VARCHAR(20),
                     parent_station      VARCHAR(20),
                     stop_timezone       VARCHAR(50),
                     wheelchair_boarding VARCHAR(20),
                     platform_code       VARCHAR(20),
                     zone_id             VARCHAR(20)
                 );`, gtfsdb);
    console.log('[Import]', 'Copying stops table');

    // await query(`create table stops as SELECT * FROM read_csv_auto('${__dirname + '/tmp'}/stops.txt', HEADER=TRUE, SAMPLE_SIZE=20000);`, gtfsdb).catch(err => {console.log(err)});

    await query(`COPY stops from '${__dirname + '/tmp'}/stops.txt';`, gtfsdb);
    // console.log('[Import]', 'Converting stops_temp');

    console.log('[Import]', 'Inserting calendar_dates');
    await query(`create table calendar as SELECT * FROM read_csv_auto('${__dirname + '/tmp'}/calendar_dates.txt', HEADER=TRUE, SAMPLE_SIZE=20000);`, gtfsdb);

    console.log('[Import]', 'Creating stop_times table');
    await query(`create table stop_times
                 (
                     trip_id             varchar(10),
                     stop_sequence       varchar(3),
                     stop_id             varchar(16),
                     stop_headsign       text,
                     arrival_time        text,
                     departure_time      text,
                     pickup_type         varchar(1),
                     drop_off_type       varchar(1),
                     timepoint           varchar(1),
                     shape_dist_traveled varchar(1),
                     fare_units_traveled varchar(1)
                 );`, gtfsdb);


    console.log('[Import]', 'Inserting stop_times');
    await query(`COPY stop_times from '${__dirname + '/tmp'}/stop_times.txt';`, gtfsdb);
    // console.log('[Import]', 'Pre-sorting stop_times');
    // await query(`SELECT * FROM stop_times ORDER BY departure_time`, gtfsdb);

    console.log('[Import]', 'Creating indexes on stop_times');
    await query('CREATE INDEX st ON stop_times (stop_id);', gtfsdb)
    console.log('[Import]', 'Creating indexes on trips');
    await query('CREATE INDEX tr ON trips (trip_id);', gtfsdb)
    console.log('[Import]', 'Creating indexes on routes');
    await query('CREATE INDEX rt ON routes (route_id);', gtfsdb);
    console.log('[Import]', 'Creating indexes on stops');
    await query('CREATE INDEX stp_id ON stops (stop_id);', gtfsdb);
    await query('CREATE INDEX stp_zi ON stops (zone_id);', gtfsdb);
    await query('CREATE INDEX stp_sc ON stops (stop_code);', gtfsdb);
    // await query('CREATE INDEX stp_ps ON stops (parent_station);', gtfsdb);
    // console.log('[Import]', 'Creating indexes on agencies');
    // await query('CREATE INDEX ag ON agency (agency_id);', gtfsdb);
    // console.log('[Import]', 'Creating indexes on calendar');
    // await query('CREATE INDEX cl ON calendar (service_id, date);', gtfsdb);

    // await query(`create table stop_times as SELECT * FROM read_csv_auto('${__dirname + '/tmp'}/stop_times.txt', HEADER=TRUE, SAMPLE_SIZE=20000);`, gtfsdb);

    // console.log('[Import]', 'Converting stop_times');
    //await query(`create table stop_times (trip_id INTEGER, stop_sequence INTEGER, stop_id varchar(16), stop_headsign text, arrival_time text, departure_time text, pickup_type INTEGER, drop_off_type INTEGER, timepoint INTEGER, shape_dist_traveled INTEGER, fare_units_traveled INTEGER);`, gtfsdb);

    // console.log('[Import]', 'Converting stop_times');
    // await query(`COPY stops from '${__dirname + '/tmp'}/stops.txt';`, gtfsdb);


    console.timeEnd('Import');


    // console.time(`Optimising`);
    // console.log('[Import]', 'Done');
    // console.log('[Optimising]', 'Creating relevants table');
    // await query(`create table stop_times_relevant
    //              (
    //                  departure_time      text,
    //                  arrival_time        text,
    //                  trip_headsign       text,
    //                  stop_id             varchar(16),
    //                  stop_sequence       varchar(3),
    //                  platform_code       varchar(5),
    //                  trip_long_name      text,
    //                  agency_name         text,
    //                  realtime_trip_id    text,
    //                  date                varchar(8)
    //              );`, gtfsdb);
    // console.log('[Optimising]', 'Inserting to relevants table');
    // await query(`COPY (SELECT departure_time, arrival_time, trip_headsign, platform_code, trip_long_name, agency_name, realtime_trip_id, date, stop_sequence, stop_id
    //              FROM stop_times join trips using (trip_id) join calendar using (service_id) join routes using (route_id) join stops using (stop_id) join agency using (agency_id)
    //              WHERE date = '20210415'
    //              ORDER BY departure_time) TO 'stop_times_relevant.csv' WITH (HEADER 1, DELIMITER ',')`, gtfsdb)
    // console.log('[Optimising]', 'Generating relevant data');
    // console.timeEnd(`Optimising`);
    // console.log('[Optimising]', 'Done');

    let app = express();

    app.get('/departures/:stopArea', async (req, res) => {
        console.time("Departures");
        // let stopsQuery = await query(`select *
        //                               from stops
        //                               WHERE zone_id = 'IFF:ah';`, gtfsdb);

        let stopArea = req.params.stopArea;

        console.time("Stops");


        let stopsQuery = await query(`select * from stop_areas WHERE code = '${stopArea}';`, gtfsdb);
        if (stopsQuery.length > 0) {
            let stopArea: {
                "code", "name", "town", "street", "lat", "lon", "type"
            } = stopsQuery[0];

            let matchStops = [];

            if(stopArea.type == 'railStation'){
                let zoneId = "IFF:" + stopArea.code.split(":S:")[stopArea.code.split(":S:").length - 1];
                matchStops = [zoneId];
            } else {
                let stops = await query(`select * from chb_stops WHERE stopArea = '${stopArea.code}';`, gtfsdb);
                matchStops = [...matchStops, ...stops.map(s => s.code.replace("S:", ""))];
            }

            console.log(matchStops);

            let q = `select * from stops WHERE (zone_id IN (${matchStops.map(s => "'" + s + "'").join(", ")}) OR (stop_code IN (${matchStops.map(s => "'" + s + "'").join(", ")})));`;

            let stops = await query(q, gtfsdb);
            console.log(stops);
            let parentStations = stops.map(s => s.parent_station);
            console.log(parentStations);

            console.timeEnd("Stops");
            console.time("Calls");


            let q2 = `SELECT departure_time as 'departureTime', trip_headsign as 'destination', platform_code as 'platform', route_short_name as 'line', trip_long_name as 'formula', agency_name as 'operator', realtime_trip_id as 'trip', date from stop_times join trips using (trip_id) join calendar using (service_id) join routes using (route_id) join stops using (stop_id) join agency using (agency_id) WHERE ((parent_station IN (${parentStations.map(s => "'" + s + "'").join(", ")}) AND date = '${moment().format("YYYYMMDD")}')) ORDER BY departure_time;`;
            let departures = await query(q2, gtfsdb);
            departures.map(d => {
                return {
                    ...d,
                    departureTime: hmsToMoment(d.departureTime, d.date).unix()
                }
            });


            console.timeEnd("Calls");
            res.send({count: departures.length, departures, stopArea});

            // let q3 = `explain SELECT departure_time as 'departureTime', trip_headsign as 'destination', platform_code as 'platform', trip_long_name as 'formula', agency_name as 'operator', realtime_trip_id as 'trip', date from stop_times join trips using (trip_id) join calendar using (service_id) join routes using (route_id) join stops using (stop_id) join agency using (agency_id) WHERE ((parent_station IN (${parentStations.map(s => "'" + s + "'").join(", ")}) AND date = '${moment().format("YYYYMMDD")}')) ORDER BY departure_time;`;
            // let explain = await query(q3, gtfsdb);
            // explain.forEach(e => {
            //     console.log(e.explain_key);
            //     console.log(e.explain_value);
            // })
            // console.log(departuresQuery.length + ' departures');
            // departuresQuery.forEach(d => {
            //     // console.log(d);
            //     console.log(`${hmsToMoment(d.departure_time, d.date).format("HH:mm:ss")} ${d.trip_headsign} ${d.platform_code}  ${d.trip_long_name} (${d.agency_name})`, JSON.stringify({
            //         realtime_trip_id: d.realtime_trip_id,
            //         date: d.date,
            //         departureTime: hmsToMoment(d.departure_time, d.date),
            //         destination: d.trip_headsign,
            //         platform: d.platform_code,
            //         formula: d.trip_long_name,
            //         operator: d.agency_name
            //     }));
            // });
            console.timeEnd("Departures");

        } else {
            res.send({error: "STOP_NOT_FOUND"});
        }
    });

    app.listen(8012, () => {
        console.log('[API]', 'Listening on :8012');
    });

    // let stops = stopsQuery.map(s => `'${s.stop_id}'`);
    // let whereQueryPart = `stop_id IN (${stops.join(", ")})`
    // let departuresQuery = await query(`select departure_time as 'departureTime', trip_headsign as 'destination', platform_code as 'platform', trip_long_name as 'formula', agency_name as 'operator', realtime_trip_id as 'trip', date from stop_times join trips using (trip_id) join calendar using (service_id) join routes using (route_id) join stops using (stop_id) join agency using (agency_id) where parent_station = 'stoparea:183059' and date = '20210415' order by departure_time;`, gtfsdb);
    // console.timeEnd("Departures");
    // console.log(departuresQuery.length + ' departures');
    // departuresQuery.forEach(d => {
    //     // console.log(d);
    //     // console.log(`${hmsToMoment(d.departureTime, d.date).format("HH:mm:ss")} ${d.destination} ${d.platform}  ${d.formula} (${d.operator})`, JSON.stringify(d));
    // })


    // console.time("Departures");
    // // let stopsQuery = await query(`select *
    // //                               from stops
    // //                               WHERE zone_id = 'IFF:ah';`, gtfsdb);
    // let stopsQuery = await query(`select *
    //                               from stops
    //                               WHERE parent_station = 'stoparea:124094';`, gtfsdb);
    // let stops = stopsQuery.map(s => `'${s.stop_id}'`);
    // let whereQueryPart = `stop_id IN (${stops.join(", ")})`
    // console.log(whereQueryPart);
    //
    // // let departuresQuery = await query(`select departure_time, trip_headsign, platform_code, trip_long_name, agency_name, realtime_trip_id, date from stop_times join trips using (trip_id) join calendar using (service_id) join routes using (route_id) join stops using (stop_id) join agency using (agency_id) where parent_station = 'stoparea:183059' and date = '20210415' order by departure_time;`, gtfsdb);
    // let q = `SELECT departure_time, trip_headsign, platform_code, trip_long_name, agency_name, realtime_trip_id, date from stop_times join trips using (trip_id) join calendar using (service_id) join routes using (route_id) join stops using (stop_id) join agency using (agency_id) WHERE ${whereQueryPart} AND date = '20210415' ORDER BY departure_time;`;
    // let departuresQuery = await query(q, gtfsdb);
    // console.timeEnd("Departures");
    // console.log(departuresQuery.length + ' departures');
    // departuresQuery.forEach(d => {
    //     // console.log(d);
    //     console.log(`${hmsToMoment(d.departure_time, d.date).format("HH:mm:ss")} ${d.trip_headsign} ${d.platform_code}  ${d.trip_long_name} (${d.agency_name})`, JSON.stringify({
    //         realtime_trip_id: d.realtime_trip_id,
    //         date: d.date,
    //         departureTime: hmsToMoment(d.departure_time, d.date),
    //         destination: d.trip_headsign,
    //         platform: d.platform_code,
    //         formula: d.trip_long_name,
    //         operator: d.agency_name
    //     }));
    // })
}

run();

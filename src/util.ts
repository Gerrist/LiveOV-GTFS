import * as moment from "moment-timezone";
moment.tz.setDefault('Europe/Amsterdam');

export function hmsToMoment(hms: string, date: string) {
    let sfds = moment.duration(hms, 'hours').asSeconds();
    return moment(date, 'YYYYMMDD').add(sfds, 'seconds').local();
}

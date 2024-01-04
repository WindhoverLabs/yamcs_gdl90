import { GDL90, GeoAltitude, Ownership, Traffic } from './node_modules/gdl90-broadcaster';
import dgram from 'dgram';

const server = new GDL90({host:"localhost", dgram, logging: true });

await server.start(heartbeat => {
	const owner = new Ownership();
	owner.callsign = 'G-WZYZ';
	owner.latitudeDeg = 29.40643619530721;
	owner.longitudeDeg = -95.02713716550542;
	owner.altitudeFt = 100;
	owner.horizontalVelocityKts = 95;
	owner.trackHeadingDeg = 10;
	owner.headingMagnetic = true;
	owner.airborne = false;

	const geoAlt = new GeoAltitude();
	geoAlt.geoAltitudeFt = 0;
	geoAlt.verticalMetrics = 1;

	const traffic = new Traffic();
	traffic.participantAddress++;
	traffic.callsign = 'N851TB';
	traffic.latitudeDeg =  29.40643619530721 ;
	traffic.longitudeDeg =  -95.12713716550542 ;
	traffic.altitudeFt = 1200;
	traffic.horizontalVelocityKts = 110;
	traffic.airborne = true;
	traffic.headingMagnetic = true;
	traffic.trackHeadingDeg = 180;

	return [owner, geoAlt, traffic];
});

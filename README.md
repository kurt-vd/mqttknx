# mqttnxd

mqttnxd is a bridge between [eibd][] and [MQTT][]

# example use
## binaries

Run these commands (or start with your init system).

	$ mqttnxd -v &

For auto-turn-off function, use mqttoff too:

	$ mqttoff -s /timeoff &

## example MQTT topic layout

* home/kitchen		__0|1__		kitchen light
* home/kitchen/eib	__x/y/z__	Linked EIB group address, with 'v' or 'a' suffixes
* home/hall		__0|1__		Hall light
* home/hall/eib		__x/y/z__	Linked EIBgroup address.
* home/hall/timeoff	__10m__		Turn off light after 10min.
* ...

# other tools
### eibgtrace

Trace EIB group address writes

### eibtimeoff

eibtimeoff is meant to turn off EIB group addresses after some timeout

It is obsoleted by mqttoff, in the mqttalrm package.


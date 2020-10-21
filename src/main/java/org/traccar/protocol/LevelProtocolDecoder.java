/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.traccar.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.buffer.Unpooled;
import java.net.SocketAddress;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Objects;
import java.util.TimeZone;
import javax.xml.bind.DatatypeConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.BaseProtocolDecoder;
import org.traccar.DeviceSession;
import org.traccar.model.Position;
import org.traccar.model.CellTower;
import org.traccar.model.Network;
import org.traccar.NetworkMessage;

/**
 *
 * @author sj
 * Version 1.0 09APR19 First release
 */
////////////////////////////////////////////////////////////////////////////////
public class LevelProtocolDecoder extends BaseProtocolDecoder {

    public enum Services {
        //                value  length (without id and service byte)
        servicePing      (0x02E, 72),
        serviceGpsSmall  (0x08F, 13),
        serviceGpsBig    (0x08A, 41),
        serviceOrOne     (0x6F,   0), // no length check
        serviceDebug     (0xFD,   0); // no length check

        private final int service;
        private final int serviceLength; // length minus deviceId length and service length

        Services(Integer service, int serviceLength) {
            this.service       = service;
            this.serviceLength = serviceLength;
        }

        public Integer getService() {
            return service;
        }

        private int getServiceLength() {
            return serviceLength;
        }

        public Services searchService(Integer serv) {

            Services result = null;
            for (Services searchService : Services.values()) {
                if (Objects.equals(searchService.getService(), serv)) {
                    result = searchService;
                    break;
                }
            }
            return result;
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(LevelProtocolDecoder.class);

    // Date formatters
    private final String dateFormat = "yyyy-MM-dd HH:mm:ss z";
    private final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(dateFormat);
    private final SimpleDateFormat  simpleDateFormat  = new SimpleDateFormat(dateFormat);

    // Time zones
    private final ZoneId utcZoneId   = ZoneId.of("UTC");
    private final ZoneId localZoneId = ZoneId.systemDefault();

    private final double latLonFactor       = 0.000025;
    private final double oneFactor          = 1.0;
    private final double courseFactor       = 2.0;
    private final double powerFactor        = 0.01;
    private final double knotsFactor        = 0.54;
    private final double hdopFactor         = 0.01;
    private final double speedFactor        = 0.01;
    private final double kmFactor           = 0.01852;
    private final double courseGroundFactor = 0.01;

    private static boolean debugSw;


////////////////////////////////////////////////////////////////////////////////
    public LevelProtocolDecoder(LevelProtocol protocol) {
        super(protocol);
        debugSw = false;
    }
////////////////////////////////////////////////////////////////////////////////
    @Override
    protected Object decode(Channel      channel,
                           SocketAddress remoteAddress,
                           Object        msg) throws Exception {

        channel.remoteAddress();

        ByteBuf buf = (ByteBuf) msg;
        Position position = null;

        // show what we received
        int readableBytes = buf.readableBytes();
        buf.markReaderIndex();
        if(debugSw) LOGGER.info("LevelProtocolDecoder readableBytes {}", readableBytes);
        buf.resetReaderIndex();

        // get level device id
        Integer levelId = decodeDeviceId(buf);
        if (levelId == null) {
            LOGGER.error("No device Level device id");
            buf.clear();
            return null;
        }
        if(debugSw) LOGGER.info("leveldId {}",levelId);

//        // get session device id
//        DeviceSession deviceSession = getDeviceSession(channel, remoteAddress, Integer.toString(levelId));
//        if (deviceSession == null) {
//            LOGGER.error("No device session device id");
//            buf.clear();
//            return null;
//        }
//        Long deviceSessionId = deviceSession.getDeviceId();
//        if(debugSw) LOGGER.info("deviceSessionId {}",deviceSessionId);

        // get and check service
        Integer serviceId = buf.readByte() & 0xFF;
        Services service = Services.servicePing.searchService(serviceId);
        if (service == null) {
            LOGGER.error(String.format("Unexpected service 0x%02X/%d",
                    serviceId, serviceId));
            buf.clear();
            return null;
        }

        // show service
        if(debugSw) LOGGER.info(String.format("LevelId %d service 0x%02X/%d %s",
                                levelId,
                                serviceId,
                                serviceId,
                                service.name()));
        
        Long deviceSessionId  = null;
        // if serviceDebug do not check leveldId
        if (!Objects.equals(serviceId, Services.serviceDebug.getService())) {

            // get session device id
            DeviceSession deviceSession = getDeviceSession(channel, remoteAddress, Integer.toString(levelId));
            if (deviceSession == null) {
                LOGGER.error("No device session device id");
                buf.clear();
                return null;
            }
            deviceSessionId = deviceSession.getDeviceId();
            if(debugSw) LOGGER.info("deviceSessionId {}",deviceSessionId);            
        }

        // check read length (minus deviceId and service length) with required service length
        int packetlength  = readableBytes - buf.readerIndex();
        int serviceLength = service.getServiceLength();

        if (serviceLength > 0 && packetlength != serviceLength) {

            LOGGER.error(String.format("Length mismatch for '%s'. Need %d read %d bytes.",
                    service.name(),
                    serviceLength,
                    packetlength));

            buf.clear();
            return null;
        }

        if (Objects.equals(serviceId, Services.servicePing.getService())) {

            position = decodePing(buf,deviceSessionId);
            sendPingReply(channel,remoteAddress);

        } else if (Objects.equals(serviceId, Services.serviceGpsSmall.getService())) {

            position = decodeGpsSmall(buf,deviceSessionId);

        } else if (Objects.equals(serviceId, Services.serviceGpsBig.getService())) {

            position = decodeGpsBig(buf,deviceSessionId);

        } else if (Objects.equals(serviceId, Services.serviceOrOne.getService())) {

            position = decodeOrOne(buf,deviceSessionId);

        } else if (Objects.equals(serviceId, Services.serviceDebug.getService())) {

            processDebug(decodeInternal(buf,deviceSessionId));       
            position = null;

        } 

        return position;
    }
////////////////////////////////////////////////////////////////////////////////
    protected Integer decodeDeviceId(ByteBuf buf) {

//    Device ID
//    binary    hex                     range
//    0b0....   0x0000     -     0x752F  7560000 -  7589999
//    0b1000.   0x800000   -   0x86418F  7590000 -  7999999
//    0b10...   0x900000   -   0xBDC6BF 70000000 - 72999999
//    0b1100.   0xC0000000 - 0xC06ACFBF 73000000 - 79999999
//    Device numbers 7561408 (0x0580), 7561440 (0x05A0) and 7561472 (0x05C0)
//    were skipped to distinguish both protocols.
//
//    0x752F = 29999
//
//    0000 0000 0000 0000 = 7560000
//    0111 0101 0010 1111 = 7589999
//    ^ = 0
//    0000 0000 0000 0000 + 7560000 = 7560000
//    0111 0101 0010 1111 + 7560000 = 7589999
//
//    0x800000 = 1000 0000 0000 0000 0000 0000 = 7590000
//    0x86418F = 1000 0110 0100 0001 1000 1111 = 7999999
//               ^ = 1000
//
//                    0000 0000 0000 0000 0000 + 7590000 = 7590000
//                    0110 0100 0001 1000 1111 + 7590000 = 7999999
//
//    0x900000 = 1001 0000 0000 0000 0000 0000 = 70000000
//    0xBDC6BF = 1011 1101 1100 0110 1011 1111 = 72999999
//               ^ = 10
//
//               0001 0000 0000 0000 0000 0000 + 68951424 = 70000000
//               0011 1101 1100 0110 1011 1111 + 68951424 = 72999999
//
//    0xC0000000 = 1100 0000 0000 0000 0000 0000 0000 0000 = 73000000
//    0xC06ACFBF = 1100 0000 0110 1010 1100 1111 1011 1111 = 79999999
//                 ^ = 1100
//
//                      0000 0000 0000 0000 0000 0000 0000 + 73000000 = 73000000
//                      0000 0110 1010 1100 1111 1011 1111 + 73000000 = 79999999

        Integer result = null;

        buf.markReaderIndex();
        byte work = buf.readByte();
        buf.resetReaderIndex();

        if ((work & 0x80) == 0) {
            result = buf.readUnsignedShort();
            result += 7560000;
        } else if ((work & 0xC0) == 0xC0) {
            result = buf.readInt();
            result &= 0xFFFFFFF;
            result += 73000000;
        } else if ((work & 0x90) == 0x90) {
            result = (buf.readByte() << 16) + buf.readUnsignedShort();
            result &= 0x3FFFFF;
            result += 68951424;
        } else if ((work & 0x80) == 0x80) {
            result = (buf.readByte() << 16) + buf.readUnsignedShort();
            result &= 0xFFFFF;
            result += 7590000;
        }

        return result;
    }
////////////////////////////////////////////////////////////////////////////////
/*

68ed2e286772289fc41202b9c6015718000306d40000c22900011137000002d6049b000800daf7c517ffffffff0000000000104fb40001
   0       4     7     1 1 1 1       1       2       2       2   3   3   3   3 3       4       4 4     5
                       0 1 2 3       7       1       5       9   1   3   5   7 8       2       6 7     0

204044650180891f8931440301764395248fffff
5               6
2               0

0x2E PING
offset size type name     description
0      4    P2T  time     inner time (RTC/UTC)
4      3    s23  lat      GPS latitude [0,000025 degrees], the highest bit set if valid
7      3    s24  lon      GPS longitude [0,000025 degrees]
10     1    u8   speed    GPS speed [km/h]
11     1    u8   course   GPS course over ground [2 degrees]
12     1    u8   io       input/output state
13     4    u32  tacho    GPS odometer [m]
17     4    u32  begin    report begin (lowest offset)
21     4    u32  end      report end (first invalid offset)
25     4    u32  count    number of records
29     2    u16  pwr1     external/main power [0.01V]
31     2    u16  pwr2     battery/secondary power [0.01V]
33     2    u16  lac      GSM Location Area Code
35     2    u16  bts      GSM Base Transceiver Station
37     1    u8   signal   GSM signal quality [0..32]
38     4    u32  groups   main group switches
42     4    u32  markers  secondary group switches
46     1    u8   battperc battery charge state [%]
47b7   4b   u4   reg      registration state (0 none, 1 home, 2 search, 3 denied, 4 unknown, 5 roam)
47b3   20b  u20  oper     GSM operator ID
50     2    u16  alt      GPS altitude [m]
52     8    pbcd imsi     IMSI - International Mobile Subscriber Identity (as Packed Binary Coded Decimal)
60     12   pbcd scid     SIM Card ID / Integrated Circuit Card Identifier (ICCID)

*/
////////////////////////////////////////////////////////////////////////////////
    private Position decodePing(ByteBuf buf,
                                Long    deviceSessionId) {

        // 28 67 72 28
        ZonedDateTime zonedDateTimeUtc = decodeUtcDate(buf);
        if(debugSw) LOGGER.info(String.format("zonedDateTimeUtc   %s", getFormattedZonedDate(zonedDateTimeUtc)));

        ZonedDateTime zonedDateTimeLocal = getLocalZonedDate(zonedDateTimeUtc);
        if(debugSw) LOGGER.info(String.format("zonedDateTimeLocal %s", getFormattedZonedDate(zonedDateTimeLocal)));

        Date dateUtc = getUtcDate(zonedDateTimeUtc);
        if(debugSw) LOGGER.info(String.format("dateUtc   %s", getFormattedDate(zonedDateTimeUtc)));

        Date dateLocal = getLocalDate(zonedDateTimeLocal);
        if(debugSw) LOGGER.info(String.format("dateLocal %s", getFormattedDate(zonedDateTimeLocal)));

        // 9f
        Boolean valid = decodeValid(buf);
        if(debugSw) LOGGER.info(String.format("valid %s", valid));

        // 9f c4 12
        Double latitude = decodeLatitude(buf);
        if(debugSw) LOGGER.info(String.format("latitude %7.5f", latitude));

        // 02 b9 c6
        Double longitude = decodeLongitude(buf);
        if(debugSw) LOGGER.info(String.format("longitude %7.5f", longitude));

        // 01
        Double speed = decodeSpeed(buf);
        if(debugSw) LOGGER.info(String.format("speed %4.2f km/h %4.2f knots", speed, speed * knotsFactor));

        // 57
        Double course = decodeCourse(buf);
        if(debugSw) LOGGER.info(String.format("course %4.2f", course));

        // 18
        Integer io = decodeIo(buf);
        if(debugSw) LOGGER.info(String.format("io %d", io));

        // 00 03 06 d4
        Integer tacho = decodeTacho(buf);
        if(debugSw) LOGGER.info(String.format("tacho %d", tacho));

        // 00 00 c2 29
        Integer begin = decodeBegin(buf);
        if(debugSw) LOGGER.info(String.format("begin %d", begin));

        // 00 01 11 37
        Integer end = decodeEnd(buf);
        if(debugSw) LOGGER.info(String.format("end %d", end));

        // 00 00 02 d6
        Integer count = decodeCount(buf);
        if(debugSw) LOGGER.info(String.format("count %d", count));

        // 04 9b
        Double pwr1 = decodePower1(buf);
        if(debugSw) LOGGER.info(String.format("pwr1 %4.2f V", pwr1));

        // 00 08
        Double pwr2 = decodePower2(buf);
        if(debugSw) LOGGER.info(String.format("pwr2 %4.2f V", pwr2));

        // 00 da
        Integer lac = decodeLac(buf);
        if(debugSw) LOGGER.info(String.format("lac %d", lac));

        // f7 c5
       Integer bts = decodeBts(buf);
       if(debugSw) LOGGER.info(String.format("bts %d", bts));

        // 17
        Integer signal = decodeSignal(buf);
        if(debugSw) LOGGER.info(String.format("signal %d", signal));

        // ff ff ff ff
        Integer groups = decodeGroups(buf);
        if(debugSw) LOGGER.info(String.format("groups 0x%04X", groups));

        // 00 00 00 00
        Integer markers = decodeMarkers(buf);
        if(debugSw) LOGGER.info(String.format("markers 0x%04X", markers));

        // 00
        Integer battPerc = decodeBattCharge(buf);
        if(debugSw) LOGGER.info(String.format("battPerc %d", battPerc));

        // 10 4f b4
        Integer reg = decodeReg(buf);
        if(debugSw) LOGGER.info(String.format("reg %d", reg));

        // 10 4f b4
        Integer oper = decodeOper(buf);
        if(debugSw) LOGGER.info(String.format("oper %d", oper));

        // 00 01
        Double alt = decodeAlt(buf);
        if(debugSw) LOGGER.info(String.format("alt %4.2f m", alt));

        // 20 40 44 65 01 80 89 1f
        String imsi = decodeImsi(buf);
        if(debugSw) LOGGER.info(String.format("imsi %s", imsi));

        // 89 31 44 03 01 76 43 95 24 8f ff ff
        String scid = decodeScid(buf);
        if(debugSw) LOGGER.info(String.format("scid %s", scid));

        Position position = new Position();

        position.setDeviceId(deviceSessionId);
        position.setId(deviceSessionId);
        position.setDeviceTime(dateUtc);
        position.setFixTime(dateUtc);
        position.setServerTime(null);
        position.setValid(valid);
        position.setLatitude(latitude);
        position.setLongitude(longitude);
        position.setAltitude(alt);  // value in meters
        position.setCourse(course);
        position.setSpeed(speed * knotsFactor);   // km to knots
        position.setProtocol(getProtocolName());
        position.setNetwork(buildNetwork(oper, lac, bts));

        position.set(Position.PREFIX_IO, io);
        position.set(Position.KEY_ODOMETER, tacho);
        position.set(Position.KEY_ROAMING, reg);
        position.set(Position.KEY_BATTERY, pwr1);
        position.set(Position.KEY_BATTERY_LEVEL, pwr2);
        position.set(Position.KEY_BATTERY_LEVEL, battPerc);
        position.set(Position.KEY_RSSI, signal);
        position.set("imsi", imsi);
        position.set("scid", scid);

        return position;
    }
////////////////////////////////////////////////////////////////////////////////
    private Position decodeGpsSmall(ByteBuf buf,
                                    Long    deviceSessionId) {

/*
68ed8f286771569fc41202b9c8003b18
   0       4     7     1 1 1
                       0 1 2

0x8F GpsSmall
offset size type name     description
0      4    P2T  time     GPS/UTC time
4      3    s23  lat      GPS latitude [0,000025 degrees], the highest bit set if valid
7      3    s24  lon      GPS longitude [0,000025 degrees]
10     1    u8   speed    GPS speed [km/h]
11     1    u8   course   GPS course over ground [2 degrees]
12     1    u8   io       input/output state
13     4    u32  tacho    GPS odometer [m]
17     4    u32  grp      groups state
21     4    u32  cntr1    counter #1
25     4    u32  cntr2    counter #2
29     4    u32  cntr3    counter #3
33     4    u32  cntr4    counter #4
*/

        // 28 67 71 56
        ZonedDateTime zonedDateTimeUtc = decodeUtcDate(buf);
        if(debugSw) LOGGER.info(String.format("zonedDateTimeUtc   %s", getFormattedZonedDate(zonedDateTimeUtc)));

        ZonedDateTime zonedDateTimeLocal = getLocalZonedDate(zonedDateTimeUtc);
        if(debugSw) LOGGER.info(String.format("zonedDateTimeLocal %s", getFormattedZonedDate(zonedDateTimeLocal)));

        Date dateUtc = getUtcDate(zonedDateTimeUtc);
        if(debugSw) LOGGER.info(String.format("dateUtc   %s", getFormattedDate(zonedDateTimeUtc)));

        Date dateLocal = getLocalDate(zonedDateTimeLocal);
        if(debugSw) LOGGER.info(String.format("dateLocal %s", getFormattedDate(zonedDateTimeLocal)));

        // 9f
        Boolean valid = decodeValid(buf);
        if(debugSw) LOGGER.info("valid {}",valid);

        // 9f c4 12
        Double latitude = decodeLatitude(buf);
        if(debugSw) LOGGER.info(String.format("latitude %7.5f",latitude));

        // 02 b9 c8
        Double longitude = decodeLongitude(buf);
        if(debugSw) LOGGER.info(String.format("longitude %7.5f",longitude));

        // 00
        Double speed = decodeSpeed(buf);
        if(debugSw) LOGGER.info(String.format("speed %7.5f",speed));

        // 3b
        Double course = decodeCourse(buf);
        if(debugSw) LOGGER.info(String.format("course %7.5f",course));

        // 18
        Integer io = decodeIo(buf);
        if(debugSw) LOGGER.info("io {}",io);

        Position position = new Position();

        position.setDeviceId(deviceSessionId);
        position.setId(deviceSessionId);
        position.setDeviceTime(dateUtc);
        position.setFixTime(dateUtc);
        position.setServerTime(null);
        position.setValid(valid);
        position.setLatitude(latitude);
        position.setLongitude(longitude);
        position.setCourse(course);
        position.setSpeed(speed * knotsFactor);   // km to knots

        position.set(Position.PREFIX_IO, io);

        return position;
    }
////////////////////////////////////////////////////////////////////////////////
    private Position decodeGpsBig(ByteBuf buf,
                                  Long    deviceSessionId) {

/*

6E803000342EA004D950000A00006430060006B10110
8DACBC0040D10041F9D0370B0000D2800D109BE2B280

offset size type  name     description
0      1    byte  hour     UTC hours
1      1    byte  min      UTC minutes
2      1    byte  sec      UTC seconds
3      2    word  ms       UTC milliseconds
5      1    byte  latDeg   degrees of latitude [0 – 90]
6      4    float latMin   minutes of latitude [0.0 – 59.999...]
10     1    byte  latDir   direction of latitude [0 – north, 1 – south]
11     1    byte  lonDeg   degrees of longitude [0 – 180]
12     4    float lonMin   minutes of longitude [0.0 – 59.999...]
16     1    byte  lonDir   direction of longitude [0 – east, 1 – west]
17     1    byte  fix      quality of data [0 – invalid; 1 – valid (GB060), 2 – 2D Fix (GC072), 3 – 3D Fix (GC072)]
18     1    byte  numSat   number of satellites used
19     2    word  hdop     horizontal DOP (dilution of precision) [0.01]
21     4    float altitude height above mean sea level [m]
25     1    char  altUnits units of height above mean sea level – character 'm' (GC072) or 'M' (GB060)
26     4    float geoSep   geoid separation [m]
30     1    char  geoUnits units of geoid separation – character 'm' (GC072) or 'M' (GB060)
31     1    bool  valid    validity flag
32     2    word  speed    ground speed [0.01 knots - multiply by 0.01852 to get kmph]
34     2    word  course   course over ground [0.01 deg]
36     1    byte  day      UTC day
37     1    byte  month    UTC month
38     1    byte  year     UTC year
47     1    byte  io       input/output state
48     1    char  key      name of used key (only terminating zero character now, but that can be removed without further warning - see 1.1)
*/

        LocalTime utcTime = decode4ByteUtcTime(buf);
        if(debugSw) LOGGER.info("utcTime {}",utcTime);

        Double latitude = decodeLatDeg(buf);
        if(debugSw) LOGGER.info(String.format("latitude %7.5f",latitude));

        Double longitude = decodeLonDeg(buf);
        if(debugSw) LOGGER.info(String.format("longitude %7.5f",longitude));

        Integer fix = decodeFix(buf);
        if(debugSw) LOGGER.info("fix {}",fix);

        Integer numSat = decodeNumSat(buf);
        if(debugSw) LOGGER.info("numSat {}",numSat);

        Double hdop = decodeHdop(buf);
        if(debugSw) LOGGER.info(String.format("hdop %5.2f",hdop));

        Double altitude = decodeAltitude(buf);
        if(debugSw) LOGGER.info(String.format("altitude %7.5f",altitude));

        Character altUnits = decodeAltUnits(buf);
        if(debugSw) LOGGER.info("altUnits {}",altUnits);

        Double geoSep = decodeGeoSep(buf);
        if(debugSw) LOGGER.info(String.format("geoSep %7.5f",geoSep));

        Character geoUnits = decodeGeoUnits(buf);
        if(debugSw) LOGGER.info("geoUnits {}",geoUnits);

        Boolean valid = decodeValidFlag(buf);
        if(debugSw) LOGGER.info("valid {}",valid);

        Double speed = decodeSpeed(buf,true);
        if(debugSw) LOGGER.info(String.format("speed %7.5f",speed));

        Double course = decodeCourseGround(buf);
        if(debugSw) LOGGER.info(String.format("course %7.5f",course));

        LocalDate utcDate = decode3ByteUtcDate(buf);
        if(debugSw) LOGGER.info("utcDate {}",utcDate);

        Integer io = decodeIoState(buf);
        if(debugSw) LOGGER.info("io {}",io);

        Character key = decodeKey(buf);
        if(debugSw) LOGGER.info(String.format("key %c",key));

        ZonedDateTime zdt = ZonedDateTime.of(utcDate,utcTime,utcZoneId);
        Date utcDateTime = Date.from(zdt.toInstant());

        Position position = new Position();

        position.setDeviceId(deviceSessionId);
        position.setId(deviceSessionId);
        position.setDeviceTime(utcDateTime);
        position.setFixTime(utcDateTime);
        position.setServerTime(null);
        position.setValid(valid);
        position.setLatitude(latitude);
        position.setLongitude(longitude);
        position.setAltitude(altitude);  // value in meters
        position.setCourse(course);
        position.setSpeed(speed * knotsFactor);   // km to knots

        position.set(Position.PREFIX_IO, io);
        position.set(Position.KEY_HDOP, hdop);

        return position;
    }
////////////////////////////////////////////////////////////////////////////////
    private Position decodeOrOne(ByteBuf buf,
                                 Long    deviceSessionId) {

        //    6E6001D00253550625366253D
        //    8DF014E669B225A29BEF49B2A

        //    Unit: 6F OR_ONE, 6D OR_FIRST, 6C OR_INNER, 6E OR_LAST On-line Report
        //    offset	size	type	name	description
        //    0	4	u32	offset	record(s) offset
        //    4	?	?	recs	records

        //    68 ed  6f     00 01 14 de  06  06   29 5b 32 52  55      0a  62   29 5b 3e 6f  64 29 5b 32 da
        //    device OR_ONE offset       len type time         data    len type time         data

        int offset = buf.readInt();
        if(debugSw) LOGGER.info("offset {}",offset);

        while(buf.readableBytes() > 0) {

            int length = buf.readByte() & 0xFF;
            if(debugSw) LOGGER.info(String.format("length 0x%02X/%d",length,length));

            int type = buf.readByte() & 0xFF;
            if(debugSw) LOGGER.info(String.format("type   0x%02X/%d",type,type));

            ZonedDateTime zonedDateTime = decodeUtcDate(buf);
            if(debugSw) LOGGER.info("zonedDateTime {}",zonedDateTime);

            int dataLength = length - 5; // 1 for type, 4 for time
            byte[] dataBytes = new byte[dataLength];

            buf.readBytes(dataBytes);
            String hexString = DatatypeConverter.printHexBinary(dataBytes);
            if(debugSw) LOGGER.info("data {}",hexString);
        }

        return null;
    }
////////////////////////////////////////////////////////////////////////////////
    private String decodeInternal(ByteBuf buf,
                                  Long    deviceSessionId) {

        String data = "";

        int dataLength = buf.readByte() & 0xFF;

        if(dataLength > 0) {

            byte[] dataBytes = new byte[dataLength];
            buf.readBytes(dataBytes);
            data = new String(dataBytes);
        }

        if(debugSw) LOGGER.info("decodeInternal length {} data '{}'",dataLength,data);

        return data;
    }
////////////////////////////////////////////////////////////////////////////////
    private void processDebug(String data) {

        if(data != null) {

            switch(data.toLowerCase()) {

                case "":
                    debugSw = !debugSw;
                    break;

                case "on":
                    debugSw = true;
                    break;

                case "off":
                    debugSw = false;
                    break;

                default:
                    LOGGER.error("Unexpected debug data {}",data);
            }
        }

        LOGGER.info("debugSw {}",debugSw);
    }
////////////////////////////////////////////////////////////////////////////////
    protected ZonedDateTime decodeUtcDate(ByteBuf buf) {

        // offset size type name     description
        // 0      4    P2T  time     inner time (RTC/UTC)

        ZonedDateTime result = null;

        int value = buf.readInt(); // 678980813

        int work = value / 60;
        int seconds = (int) value % 60;
        int minutes = (int) work % 60;
        work = work / 60;
        int hours   = (int) work % 24;
        work = work / 24;
        int day = (int) work % 32;
        work = work / 32;
        int month = (int) work % 13;
        int year = (int) work / 13;

        LOGGER.debug(String.format("year %d month %d day %d hours %d minutes %d seconds %d",
                year, month, day, hours, minutes, seconds));

        // create UTC ZonedDateTime
        result = ZonedDateTime.of(year + 2000, month, day, hours, minutes, seconds, 0, utcZoneId);

        return result;
    }
////////////////////////////////////////////////////////////////////////////////
    public ZonedDateTime getLocalZonedDate(ZonedDateTime zonedDateTime) {
        return zonedDateTime.withZoneSameInstant(localZoneId);
    }
////////////////////////////////////////////////////////////////////////////////
    public String getFormattedZonedDate(ZonedDateTime zonedDateTime) {
        return dateTimeFormatter.format(zonedDateTime);
    }
////////////////////////////////////////////////////////////////////////////////
    public Date getUtcDate(ZonedDateTime zonedDateTimeUtc) {
        return Date.from(zonedDateTimeUtc.toInstant());
    }
////////////////////////////////////////////////////////////////////////////////
    public Date getLocalDate(ZonedDateTime zonedDateTimeUtc) {
        return Date.from(getUtcDate(zonedDateTimeUtc).toInstant());
    }
////////////////////////////////////////////////////////////////////////////////
    public String getFormattedDate(ZonedDateTime zonedDateTime) {
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone(zonedDateTime.getZone()));
        return simpleDateFormat.format(Date.from(zonedDateTime.toInstant()));
    }
////////////////////////////////////////////////////////////////////////////////
    protected Boolean decodeValid(ByteBuf buf) {

        // offset size type name     description
        // 4      3    s23  lat      GPS latitude [0,000025 degrees], the highest bit set if valid

        Boolean result = false;

        buf.markReaderIndex();
        byte work = buf.readByte();
        buf.resetReaderIndex();

        result = (work & 0x80) == 0x80;

        return result;
    }
////////////////////////////////////////////////////////////////////////////////
    protected Double decodeLatitude(ByteBuf buf) {

        // offset size type name     description
        // 4      3    s23  lat      GPS latitude [0,000025 degrees], the highest bit set if valid

        Double result = null;

        int work = (buf.readByte() << 16) + buf.readUnsignedShort();
        work &= 0x7FFFFF;
        result = work * latLonFactor;

        return result;
    }
////////////////////////////////////////////////////////////////////////////////
    protected Double decodeLongitude(ByteBuf buf) {

        // offset size type name     description
        // 7      3    s24  lon      GPS longitude [0,000025 degrees]

        Double result = null;

        int work = (buf.readByte() << 16) + buf.readUnsignedShort();
        result = work * latLonFactor;

        return result;
    }
////////////////////////////////////////////////////////////////////////////////
    protected Double decodeSpeed(ByteBuf buf) {

        // offset size type name     description
        // 10     1    u8   speed    GPS speed [km/h]

        Double result = null;

        int work = buf.readByte();
        result = work * oneFactor;

        return result;
    }
////////////////////////////////////////////////////////////////////////////////
    protected Double decodeCourse(ByteBuf buf) {

        // offset size type name     description
        // 11     1    u8   course   GPS course over ground [2 degrees]

        Double result = null;

        int work = buf.readByte() & 0xFF;
        result = work * courseFactor;

        return result;
    }
////////////////////////////////////////////////////////////////////////////////
    protected Integer decodeIo(ByteBuf buf) {

        // offset size type name     description
        // 12     1    u8   io       input/output state

        Integer result = buf.readByte() & 0xFF;

        return result;
    }
////////////////////////////////////////////////////////////////////////////////
    protected Integer decodeTacho(ByteBuf buf) {

        // offset size type name     description
        // 13     4    u32  tacho    GPS odometer [m]

        Integer result = buf.readInt();

        return result;
    }
////////////////////////////////////////////////////////////////////////////////
    protected Integer decodeBegin(ByteBuf buf) {

        // offset size type name     description
        // 17     4    u32  begin    report begin (lowest offset)

        Integer result = buf.readInt();

        return result;
    }
////////////////////////////////////////////////////////////////////////////////
    protected Integer decodeEnd(ByteBuf buf) {

        // offset size type name     description
        // 21     4    u32  end      report end (first invalid offset)

        Integer result = buf.readInt();

        return result;
    }
////////////////////////////////////////////////////////////////////////////////
    protected Integer decodeCount(ByteBuf buf) {

        // offset size type name     description
        // 25     4    u32  count    number of records

        Integer result = buf.readInt();

        return result;
    }
////////////////////////////////////////////////////////////////////////////////
    protected Double decodePower1(ByteBuf buf) {

        // offset size type name     description
        // 29     2    u16  pwr1     external/main power [0.01V]

        Double result = buf.readShort() * powerFactor;

        return result;
    }
////////////////////////////////////////////////////////////////////////////////
    protected Double decodePower2(ByteBuf buf) {

        // offset size type name     description
        // 31     2    u16  pwr2     battery/secondary power [0.01V]

        Double result = buf.readShort() * powerFactor;

        return result;
    }
////////////////////////////////////////////////////////////////////////////////
    protected Integer decodeLac(ByteBuf buf) {

        // offset size type name     description
        // 33     2    u16  lac      GSM Location Area Code

        int work = buf.readShort();
        Integer result = work;

        return result;
    }
////////////////////////////////////////////////////////////////////////////////
    protected Integer decodeBts(ByteBuf buf) {

        // offset size type name     description
        // 35     2    u16  bts      GSM Base Transceiver Station

       Integer result = buf.readShort() & 0xFFFF;

       return result;
    }
////////////////////////////////////////////////////////////////////////////////
    protected Integer decodeSignal(ByteBuf buf) {

        // offset size type name     description
        // 37     1    u8   signal   GSM signal quality [0..32]

        Integer result = buf.readByte() & 0xFF;

        return result;
    }
////////////////////////////////////////////////////////////////////////////////
    protected Integer decodeGroups(ByteBuf buf) {

        // offset size type name     description
        // 38     4    u32  groups   main group switches

        Integer result = buf.readInt();

        return result;
    }
////////////////////////////////////////////////////////////////////////////////
    protected Integer decodeMarkers(ByteBuf buf) {

        // offset size type name     description
        // 42     4    u32  markers  secondary group switches

        Integer result = buf.readInt();

        return result;
    }
////////////////////////////////////////////////////////////////////////////////
    protected Integer decodeBattCharge(ByteBuf buf) {

        // offset size type name     description
        // 46     1    u8   battperc battery charge state [%]

        Integer result = buf.readByte() & 0xFF;

        return result;
    }
////////////////////////////////////////////////////////////////////////////////
    protected Integer decodeReg(ByteBuf buf) {

        // offset size type name     description
        // 47b7   4b   u4   reg      registration state (0 none, 1 home, 2 search, 3 denied, 4 unknown, 5 roam)

        buf.markReaderIndex();
        byte work = buf.readByte();
        buf.resetReaderIndex();

        Integer result = (work >> 4) & 0xF;

        return result;
    }
////////////////////////////////////////////////////////////////////////////////
    protected Integer decodeOper(ByteBuf buf) {

        // offset size type name     description
        // 47b3   20b  u20  oper     GSM operator ID

        Integer result = ((buf.readByte() & 0x0F) << 16) | buf.readShort();

        return result;
    }
////////////////////////////////////////////////////////////////////////////////
    protected Double decodeAlt(ByteBuf buf) {

        // offset size type name     description
        // 50     2    u16  alt      GPS altitude [m]

        Double result = buf.readShort() * oneFactor;

        return result;
    }
////////////////////////////////////////////////////////////////////////////////
    protected String decodeImsi(ByteBuf buf) {

        // offset size type name     description
        // 52     8    pbcd imsi     IMSI - International Mobile Subscriber Identity (as Packed Binary Coded Decimal)

        return decodePackedBinaryCodedDecimal(buf, 8);
    }
////////////////////////////////////////////////////////////////////////////////
    protected String decodeScid(ByteBuf buf) {

        // offset size type name     description
        // 60     12   pbcd scid     SIM Card ID / Integrated Circuit Card Identifier (ICCID)

       return decodePackedBinaryCodedDecimal(buf, 12);
    }
////////////////////////////////////////////////////////////////////////////////
    private String decodePackedBinaryCodedDecimal(ByteBuf buf,
                                                  int     noBytes) {

        // imsi : 20 40 44 65 01 80 89 1f
        // scid : 89 31 44 03 01 76 43 95 24 8f ff ff

        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < noBytes; i++) {

            byte b = buf.readByte();

            int left = b >> 4 & 0xF;
            if (left >= 0 && left <= 9) {
                sb.append((char) ('0' + left));
            }

            int right = b & 0xF;
            if (right >= 0 && right <= 9) {
                sb.append((char) ('0' + right));
            }
        }

        return sb.toString();
    }
////////////////////////////////////////////////////////////////////////////////
    private Network buildNetwork(int oper,
                                 int lac,
                                 int bts) {

        // https://cellidfinder.com/cells

        int mcc = oper / 100;
        int mnc = oper % 100;

        return new Network(CellTower.from(mcc, mnc, lac, bts));
    }
////////////////////////////////////////////////////////////////////////////////
    private LocalTime decode4ByteUtcTime(ByteBuf buf) {

        //    offset size type  name     description
        //    0      1    byte  hour     UTC hours
        //    1      1    byte  min      UTC minutes
        //    2      1    byte  sec      UTC seconds
        //    3      2    word  ms       UTC milliseconds

        LocalTime result = null;

        int hour        = buf.readByte();
        int minute      = buf.readByte();
        int second      = buf.readByte();
        int millisecond = buf.readShort();

        result = LocalTime.of(hour, minute, second, millisecond * 1000000);

        return result;
    }
////////////////////////////////////////////////////////////////////////////////
    private Double decodeLatDeg(ByteBuf buf) {

        //    offset size type  name     description
        //    5      1    byte  latDeg   degrees of latitude [0 – 90]
        //    6      4    float latMin   minutes of latitude [0.0 – 59.999...]
        //    10     1    byte  latDir   direction of latitude [0 – north, 1 – south]

        Double result = null;

        int   latDeg = buf.readByte();  // 52
        float latMin = buf.readFloat(); // 2.711988
        int   latDir = buf.readByte();

        result = latDeg * 1.0 + (latMin / 60); // 52.04519980028272

        if(latDir == 1)
            result *= -1;

        return result;
    }
////////////////////////////////////////////////////////////////////////////////
    private Double decodeLonDeg(ByteBuf buf) {

        //    offset size type  name     description
        //    11     1    byte  lonDeg   degrees of longitude [0 – 180]
        //    12     4    float lonMin   minutes of longitude [0.0 – 59.999...]
        //    16     1    byte  lonDir   direction of longitude [0 – east, 1 – west]

        Double result = null;

        int   lonDeg = buf.readByte();   // 4
        float lonMin = buf.readFloat();  // 27.949657
        int   lonDir = buf.readByte();

        result = lonDeg * 1.0 + (lonMin / 60); // 4.465827614068985

        if(lonDir == 1)
            result *= -1;

        return result;
    }
////////////////////////////////////////////////////////////////////////////////
    private Integer decodeFix(ByteBuf buf) {

        //    offset size type  name     description
        //    17     1    byte  fix      quality of data [0 – invalid; 1 – valid (GB060), 2 – 2D Fix (GC072), 3 – 3D Fix (GC072)]

        Integer result = null;

        int fix = buf.readByte();
        result = fix;

        return result;
    }
////////////////////////////////////////////////////////////////////////////////
    private Integer decodeNumSat(ByteBuf buf) {

        //    offset size type  name     description
        //    18     1    byte  numSat   number of satellites used

        Integer result = null;

        int numSat = buf.readByte();
        result = numSat;

        return result;
    }
////////////////////////////////////////////////////////////////////////////////
    private Double decodeHdop(ByteBuf buf) {

        //    offset size type  name     description
        //    19     2    word  hdop     horizontal DOP (dilution of precision) [0.01]

        Double result = null;

        int hdop = buf.readShort();
        result = hdop * hdopFactor;

        return result;
    }
////////////////////////////////////////////////////////////////////////////////
    private Double decodeAltitude(ByteBuf buf) {

        //    offset size type  name     description
        //    21     4    float altitude height above mean sea level [m]

        Double result = null;

        float altitude = buf.readFloat();
        result = (double) altitude;

        return result;
    }
////////////////////////////////////////////////////////////////////////////////
    private Character decodeAltUnits(ByteBuf buf) {

        //    offset size type  name     description
        //    25     1    char  altUnits units of height above mean sea level – character 'm' (GC072) or 'M' (GB060)

        Character result= null;

        int altUnits = buf.readByte();
        result = (char) altUnits;

        return result;
    }
////////////////////////////////////////////////////////////////////////////////
    private Double decodeGeoSep(ByteBuf buf) {

        //    offset size type  name     description
        //    26     4    float geoSep   geoid separation [m]

        Double result = null;

        float geoSep = buf.readFloat();
        result = (double) geoSep;

        return result;
    }
////////////////////////////////////////////////////////////////////////////////
    private Character decodeGeoUnits(ByteBuf buf) {

        //    offset size type  name     description
        //    30     1    char  geoUnits units of geoid separation – character 'm' (GC072) or 'M' (GB060)

        Character result= null;

        int geoUnits = buf.readByte();
        result = (char) geoUnits;

        return result;
    }
////////////////////////////////////////////////////////////////////////////////
    private Boolean decodeValidFlag(ByteBuf buf) {

        //    offset size type  name     description
        //    31     1    bool  valid    validity flag

        Boolean result = null;

        byte validFlag = buf.readByte();
        result = validFlag != 0;

        return result;
    }
////////////////////////////////////////////////////////////////////////////////
    private Double decodeSpeed(ByteBuf buf,
                               boolean kmSw) {

        //    offset size type  name     description
        //    32     2    word  speed    ground speed [0.01 knots - multiply by 0.01852 to get kmph]

        Double result = null;

        int speed = buf.readShort();
        result = speed * (kmSw == false ? speedFactor : kmFactor);

        return result;
    }
////////////////////////////////////////////////////////////////////////////////
    private Double decodeCourseGround(ByteBuf buf) {

        //    offset size type  name     description
        //    34     2    word  course   course over ground [0.01 deg]

        Double result = null;

        int course = buf.readShort();
        result = course * courseGroundFactor;

        return result;
    }
////////////////////////////////////////////////////////////////////////////////
    private LocalDate decode3ByteUtcDate(ByteBuf buf) {

        //    offset size type  name     description
        //    36     1    byte  day      UTC day
        //    37     1    byte  month    UTC month
        //    38     1    byte  year     UTC year

        LocalDate result = null;

        int day   = buf.readByte();
        int month = buf.readByte();
        int year  = buf.readByte();

        result = LocalDate.of(2000 + year,month,day);

        return result;
    }
////////////////////////////////////////////////////////////////////////////////
    private Integer decodeIoState(ByteBuf buf) {

        //    offset size type  name     description
        //    47     1    byte  io       input/output state

        Integer result = null;

        int io = buf.readByte();
        result = io;

        return result;
    }
////////////////////////////////////////////////////////////////////////////////
    private Character decodeKey(ByteBuf buf) {

        //    offset size type  name     description
        //    48     1    char  key      name of used key (only terminating zero character now, but that can be removed without further warning - see 1.1)

        Character result= null;

        int key = buf.readByte();
        result = (char) key;

        return result;
    }
////////////////////////////////////////////////////////////////////////////////
    private void sendPingReply(Channel       channel,
                               SocketAddress remoteAddress) {

        if (channel != null && remoteAddress != null) {
            ByteBuf reply = Unpooled.buffer();
            reply.writeByte(Services.servicePing.getService());
            channel.writeAndFlush(new NetworkMessage(reply, remoteAddress));
        }
    }
////////////////////////////////////////////////////////////////////////////////
}
////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

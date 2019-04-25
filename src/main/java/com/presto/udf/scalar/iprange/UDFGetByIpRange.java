package com.presto.udf.scalar.iprange;

import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.util.concurrent.locks.ReentrantLock;

public class UDFGetByIpRange  {

    private static final Logger LOG = Logger.get(UDFGetByIpRange.class);

    static {
        LOG.info("17monipdb.datx not load ,load it  ");
        IPExt.load();
        LOG.info("17monipdb.datx load finish ");
    }

    @ScalarFunction("find_range_by_ip")
    @Description("find_range_by_ip ,first arg is ip ,second is type")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice eval( @SqlType(StandardTypes.VARCHAR) Slice ips,  @SqlType(StandardTypes.VARCHAR) Slice types) {
        String ip = ips.toStringUtf8();
        String type = types.toStringUtf8();
        String result = "";
        switch (type) {
            case "country":
                result = findCountry(ip);
                break;
            case "province":
                result = findProvince(ip);
                break;
            case "city":
                result = findCity(ip);
                break;
            case "place":
                result = findPlace(ip);
                break;
            case "isp":
                result = findIsp(ip);
                break;
            case "latitude":
                result = findLatitude(ip);
                break;
            case "longitude":
                result = findLongitude(ip);
                break;
            case "timezone1":
                result = findTimezone1(ip);
                break;
            case "timezone2":
                result = findTimezone2(ip);
                break;
            case "chn_area_code":
                result = findCHNAdminAreaCode(ip);
                break;
            case "inter_phone_code":
                result = findInterPhoneCode(ip);
                break;
            case "country_code":
                result = findCountryCode(ip);
                break;
            case "world_code":
                result = findWCCode(ip);
                break;
            default:
                break;
        }
        return Slices.utf8Slice(result);
    }

    // 国家
    public static  String findCountry(String ip) {
        return IPExt.find(ip)[0];
    }

    // 省份（国内）
    public static   String findProvince(String ip) {
        return IPExt.find(ip)[1];
    }

    // 城市（国内）
    public static   String findCity(String ip) {
        return IPExt.find(ip)[2];
    }

    // 学校或单位（国内）
    public static   String findPlace(String ip) {
        return IPExt.find(ip)[3];
    }

    // 运营商
    public static   String findIsp(String ip) {
        return IPExt.find(ip)[4];
    }

    // 纬度
    public static   String findLatitude(String ip) {
        return IPExt.find(ip)[5];
    }

    // 经度
    public static   String findLongitude(String ip) {
        return IPExt.find(ip)[6];
    }

    // 时区1
    public static   String findTimezone1(String ip) {
        return IPExt.find(ip)[7];
    }

    // 时区2
    public static   String findTimezone2(String ip) {
        return IPExt.find(ip)[8];
    }

    // 中国行政区划分
    public static   String findCHNAdminAreaCode(String ip) {
        return IPExt.find(ip)[9];
    }

    // 国际电话代码
    public static   String findInterPhoneCode(String ip) {
        return IPExt.find(ip)[10];
    }

    // 国家二位代码
    public static   String findCountryCode(String ip) {
        return IPExt.find(ip)[11];
    }

    // 世界大洲代码
    public static   String findWCCode(String ip) {
        return IPExt.find(ip)[12];
    }
}

class IPExt {
    private static final Logger LOG = Logger.get(IPExt.class);
    private static int offset;
    private static int[] index = new int[65536];
    private static ByteBuffer dataBuffer;
    private static ByteBuffer indexBuffer;
    private static ReentrantLock lock = new ReentrantLock();

    public static String[] find(String ip) {
        String[] ips = ip.split("\\.");
        int prefix_value = (Integer.valueOf(ips[0]) * 256 + Integer.valueOf(ips[1]));
        long ip2long_value = ip2long(ip);
        int start = index[prefix_value];
        int max_comp_len = offset - 262144 - 4;
        long tmpInt;
        long index_offset = -1;
        int index_length = -1;
        byte b = 0;
        for (start = start * 9 + 262144; start < max_comp_len; start += 9) {
            tmpInt = int2long(indexBuffer.getInt(start));
            if (tmpInt >= ip2long_value) {
                index_offset = bytesToLong(b, indexBuffer.get(start + 6), indexBuffer.get(start + 5), indexBuffer.get(start + 4));
                index_length = ((0xFF & indexBuffer.get(start + 7)) << 8) + (0xFF & indexBuffer.get(start + 8));
                break;
            }
        }
        byte[] areaBytes = new byte[0];
        lock.lock();
        try {
            dataBuffer.position(offset + (int) index_offset - 262144);
            areaBytes = new byte[index_length];
            dataBuffer.get(areaBytes, 0, index_length);
        } catch (Exception e) {
            LOG.error("find error",e);
        } finally {
            lock.unlock();
        }
        return new String(areaBytes, Charset.forName("UTF-8")).split("\t", -1);
    }

    public static void load() {
        lock.lock();
        InputStream in = null;
        try {
            in = UDFGetByIpRange.class.getClassLoader().getResourceAsStream("17monipdb.datx");
            byte[] bytes = new byte[in.available()];
            int readBytes = 0;
            int len = bytes.length;
            while (readBytes < len) {
                int read = in.read(bytes, readBytes, len - readBytes);
                if (read == -1) {
                    break;
                }
                readBytes += read;
            }
            dataBuffer = ByteBuffer.wrap(bytes);
            dataBuffer.position(0);
            offset = dataBuffer.getInt(); // indexLength
            byte[] indexBytes = new byte[offset];
            dataBuffer.get(indexBytes, 0, offset - 4);
            indexBuffer = ByteBuffer.wrap(indexBytes);
            indexBuffer.order(ByteOrder.LITTLE_ENDIAN);

            for (int i = 0; i < 256; i++) {
                for (int j = 0; j < 256; j++) {
                    index[i * 256 + j] = indexBuffer.getInt();
                }
            }
            indexBuffer.order(ByteOrder.BIG_ENDIAN);
        } catch (IOException e) {
            LOG.error("load error",e);
        } finally {
            lock.unlock();
            try {
                in.close();
            } catch (IOException e) {
                LOG.error("load error", e);
            }
        }
    }


    private static long bytesToLong(byte a, byte b, byte c, byte d) {
        return int2long((((a & 0xff) << 24) | ((b & 0xff) << 16) | ((c & 0xff) << 8) | (d & 0xff)));
    }

    private static int str2Ip(String ip) {
        String[] ss = ip.split("\\.");
        int a, b, c, d;
        a = Integer.parseInt(ss[0]);
        b = Integer.parseInt(ss[1]);
        c = Integer.parseInt(ss[2]);
        d = Integer.parseInt(ss[3]);
        return (a << 24) | (b << 16) | (c << 8) | d;
    }

    private static long ip2long(String ip) {
        return int2long(str2Ip(ip));
    }

    private static long int2long(int i) {
        long l = i & 0x7fffffffL;
        if (i < 0) {
            l |= 0x080000000L;
        }
        return l;
    }


}
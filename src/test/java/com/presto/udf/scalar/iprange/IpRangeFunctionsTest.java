package com.presto.udf.scalar.iprange;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.junit.Assert;
import org.junit.Test;

public class IpRangeFunctionsTest {

    @Test
    public void testFindCountry()  {
        Slice result = UDFGetByIpRange.eval(Slices.utf8Slice("223.104.101.190"),Slices.utf8Slice("city")) ;
        Assert.assertEquals("中国", result.toStringUtf8());
    }

}

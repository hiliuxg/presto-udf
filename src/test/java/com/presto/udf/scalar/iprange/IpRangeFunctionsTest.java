package com.presto.udf.scalar.iprange;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.junit.Assert;
import org.junit.Test;

public class IpRangeFunctionsTest {

    @Test
    public void testFindCountry()  {
        Slice result = UDFGetByIpRange.eval(Slices.utf8Slice("117.136.45.19"),Slices.utf8Slice("province")) ;
        Assert.assertEquals("江苏", result.toStringUtf8());
    }

}

package com.presto.udf;

import com.facebook.presto.spi.Plugin;
import com.google.common.collect.ImmutableSet;
import com.presto.udf.scalar.iprange.UDFGetByIpRange;

import java.util.Set;

public class CustomFunctionPlugin implements Plugin {

    public Set<Class<?>> getFunctions() {
        return ImmutableSet.<Class<?>>builder()
                .add(UDFGetByIpRange.class)
                .build();
    }


}

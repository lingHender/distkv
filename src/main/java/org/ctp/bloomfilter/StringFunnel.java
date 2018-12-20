package org.ctp.bloomfilter;

import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

/**
 * Created by lfli on 14/08/2018.
 */
public enum  StringFunnel implements Funnel<String> {

    INSTANCE;

    @Override
    public void funnel(String s, PrimitiveSink primitiveSink) {

    }

    public static StringFunnel getInstance() {
        return INSTANCE;
    }

}

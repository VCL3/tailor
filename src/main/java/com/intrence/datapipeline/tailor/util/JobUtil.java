/**
 * Created by wliu on 12/13/17.
 */
package com.intrence.datapipeline.tailor.util;

import com.google.common.collect.ImmutableMultimap;

public class JobUtil {

    public static String extractParameterFromParameters(String parameterName, ImmutableMultimap<String, String> parameters) {
        return parameters.get(parameterName).asList().get(0);
    }
}

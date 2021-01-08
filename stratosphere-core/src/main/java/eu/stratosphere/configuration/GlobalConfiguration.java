package eu.stratosphere.configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * TODO
 *
 * @author Administrator
 * @version 1.0
 * @date 2021/01/03 23:05
 */
public final class GlobalConfiguration {

    private static GlobalConfiguration configuration = null;

    private final Map<String, String> confData = new HashMap<>();

    private static synchronized GlobalConfiguration get(){
        if(configuration == null){
            configuration = new GlobalConfiguration();
        }
        return configuration;
    }

    public static long getLong(String key, final long defaultValue){
        return get().getLongInternal(key, defaultValue);
    }

    public static int getInteger(String key, final int defaultValue) {
        return get().getIntegerInternal(key, defaultValue);
    }

    private Integer getIntegerInternal(String key, int defaultValue){
        int retVal = defaultValue;
        synchronized (confData){
            if(confData.containsKey(key)){
                retVal = Integer.parseInt(confData.get(key));
            }
        }

        return retVal;
    }

    private long getLongInternal(String key, long defaultValue){
        long retVal = defaultValue;
        synchronized (confData){
            if(this.confData.containsKey(key)){
                retVal = Long.parseLong(confData.get(key));
            }
        }

        return retVal;
    }
}

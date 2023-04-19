package com.alpha.mongodb.sharding.example.utils;

import com.alpha.mongodb.sharding.example.CucumberTestConstants;
import lombok.experimental.UtilityClass;
import okhttp3.HttpUrl;

@UtilityClass
public class ExampleServiceClientUtils {

    public static HttpUrl.Builder baseUrlBuilder() {
        return new HttpUrl.Builder()
                .scheme(CucumberTestConstants.SCHEME)
                .host(CucumberTestConstants.BASE_HOSTNAME)
                .port(CucumberTestConstants.PORT)
                .addPathSegment(CucumberTestConstants.BASE_PATH);
    }

}

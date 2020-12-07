package com.capitalone.dashboard;

import com.capitalone.dashboard.event.BuildEventListener;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;

import javax.net.ssl.HttpsURLConnection;

/**
 * Application configuration and bootstrap
 */
@SpringBootApplication
@ComponentScan(excludeFilters = {@ComponentScan.Filter(value = BuildEventListener.class, type = FilterType.ASSIGNABLE_TYPE)})
public class Application {

    public static void main(String[] args) {
        HttpsURLConnection.setDefaultHostnameVerifier(new NoopHostnameVerifier());
        SpringApplication.run(Application.class, args);
    }
}

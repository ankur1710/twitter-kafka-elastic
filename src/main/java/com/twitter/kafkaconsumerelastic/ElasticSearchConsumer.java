package com.twitter.kafkaconsumerelastic;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.stereotype.Service;


@Service
public class ElasticSearchConsumer {
    private String hostname = "kafka-learning-4199569743.us-east-1.bonsaisearch.net";
    private String username = "wabsbvkr5y";
    private String password = "dd006fpjas";

    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();

    public RestHighLevelClient getElasticSearchClient(){
        credentialsProvider.setCredentials(AuthScope.ANY,new UsernamePasswordCredentials(username,password));

        //here we are saying connect the HTTP hostname and port with the creds provided
        RestClientBuilder builder = RestClient.builder(new HttpHost(hostname,443,"https")).
                setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        return new RestHighLevelClient(builder);
    }
}

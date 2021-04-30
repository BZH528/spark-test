package com.atguigu.gmall.publisher.service.impl;

import com.atguigu.gmall.publisher.service.ESService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: bizh
 * @Date: 2021/4/29 19:16
 * @Description:
 */
@Service
public class ESServiceImpl implements ESService {

    // 将ES客户端对象注入到service中
    @Autowired
    JestClient jestClient;


    /**
     *  GET /gmall0523_dau_info_2021-04-29-query/_search
     {

     "query": {
     "match_all": {}
     }
     }
     * @param date
     * @return
     */
    @Override
    public Long getDauTotal(String date) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        searchSourceBuilder.query(new MatchAllQueryBuilder());
        String query = searchSourceBuilder.toString();
        String indexName = "gmall0523_dau_info_" + date + "-query";
        Search search = new Search.Builder(query).addIndex(indexName).build();
        Long total = 0L;

        try {
            SearchResult result = jestClient.execute(search);
            total = result.getTotal();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("查询ES失败");
        }

        return total;
    }

    /**
     * ES查询语句：
     *  GET /gmall0523_dau_info_2021-04-29-query/_search
         {
         "aggs": {
         "groupBy_hr": {
         "terms": {
         "field": "hr",
         "size": 24
         }
         }
         }
         }
     *
     *
     * @param date
     * @return
     */
    @Override
    public Map<String, Long> getDauHour(String date) {

        Map<String, Long> hrMap = new HashMap<>();

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        TermsAggregationBuilder termsAggBuilder = new TermsAggregationBuilder("groupBy_hr", ValueType.LONG).field("hr").size(24);
        sourceBuilder.aggregation(termsAggBuilder);
        String query = sourceBuilder.toString();
        String indexName = "gmall0523_dau_info_" + date + "-query";
        Search search = new Search.Builder(query)
                .addIndex(indexName)
                .build();
        try {
            SearchResult result = jestClient.execute(search);
            TermsAggregation termsAggregation = result.getAggregations().getTermsAggregation("groupBy_hr");
            if (termsAggregation != null) {
                List<TermsAggregation.Entry> buckets = termsAggregation.getBuckets();
                for (TermsAggregation.Entry bucket : buckets) {
                    hrMap.put(bucket.getKey(), bucket.getCount());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("ES查询异常");
        }
        return hrMap;
    }
}

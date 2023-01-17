package cn.itcast.hotel;

import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.impl.HotelService;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;

@SpringBootTest
public class HotelIndexTest {
    private RestHighLevelClient client;


    @Autowired
    private HotelService hotelService;



    @Test
    void testInit(){
        System.out.println(client);
    }

    @Test
    void testCreateHotelIndex() throws IOException{
        GetIndexRequest request = new GetIndexRequest("123");
//        request.source(HotelConstants.MAPPING_TEMPLATE, XContentType.JSON);
//        client.indices().create(request, RequestOptions.DEFAULT);

        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        System.err.println(exists);
    }

    @Test
    void addAll() throws IOException {

        BulkRequest request = new BulkRequest();

        List<Hotel> hotels = hotelService.list();

        for (Hotel hotel : hotels) {
            HotelDoc hotelDoc = new HotelDoc(hotel);
            request.add(new IndexRequest("hotel").id(hotelDoc.getId().toString()).source(JSON.toJSONString(hotelDoc),
                    XContentType.JSON));
        }

        client.bulk(request,RequestOptions.DEFAULT);
    }


    @Test
    void testAddDocument() throws IOException {
        // 1.根据id查询酒店数据
        Hotel hotel = hotelService.getById(61083L);
        // 2.转换为文档类型
        HotelDoc hotelDoc = new HotelDoc(hotel);
        // 3.将HotelDoc转json
        String json = JSON.toJSONString(hotelDoc);

        // 1.准备Request对象
        IndexRequest request = new IndexRequest("hotel").id(hotelDoc.getId().toString());
        // 2.准备Json文档
        request.source(json, XContentType.JSON);
        // 3.发送请求
        client.index(request, RequestOptions.DEFAULT);
    }


    @BeforeEach
    void beforeEach() {
        this.client = new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://1.14.76.222:9200")

        ));
        System.out.println("1231232123");
    }

    @AfterEach
    void afterEach() throws IOException {
        this.client.close();
    }
}

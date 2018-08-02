import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

public class Main {

    private static ArrayList<String> KEY_WORD = new ArrayList<>();
    private static String HBASE_TABLE_NAME = "g3:logs";
    private static String SPARK_APP_NAME = "FbLogFilter";
//    private static String SPARK_MASTER_OPT = "local[2]";
    private static String ELASTIC_CLUSTER_NAME = "elasticsearch";
    private static String ELASTIC_ADD = "192.168.23.64";
    private static String ELASTIC_INDEX = "fb_content_filtered";
    private static String ELASTIC_TYPE = "by_keyword";


    private static Configuration hbConfig;
    private static Connection hbConnect;

    private static JavaSparkContext sparkContext;
    private static Client client;

    private static void init() {
        // add key_word

//        StringBuilder index_type = new StringBuilder();
//        for (String key : KEY_WORD) {
//            index_type.append(key).append("_");
//        }
//        ELASTIC_TYPE = index_type.toString();
        hbConfig = HBaseConfiguration.create();
        hbConfig.set(TableInputFormat.INPUT_TABLE, HBASE_TABLE_NAME);
//        Scan scan = new Scan();
//        scan.setLimit(50000);

        try {
//            hbConfig.set(TableInputFormat.SCAN, TableMapReduceUtil.convertScanToString(scan));
            hbConnect = ConnectionFactory.createConnection(hbConfig);
        } catch (IOException e) {
            e.printStackTrace();
        }
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName(SPARK_APP_NAME)
//                .setMaster(SPARK_MASTER_OPT)
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//        sparkConf.set("es.nodes", ELASTIC_ADD)
//                .set("es.port", "9300");

        sparkContext = new JavaSparkContext(sparkConf);

        Settings settings = Settings.builder()
                .put("cluster.name", ELASTIC_CLUSTER_NAME).build();
        System.setProperty("es.set.netty.runtime.available.processors", "false");
        try {
            client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new TransportAddress(InetAddress.getByName(ELASTIC_ADD), 9300));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    private static void closeSpark() {
        sparkContext.stop();
    }

    private static void closeElastic() {
        client.close();
    }

    public static void main(String[] args) {
        init();
        Collections.addAll(KEY_WORD, args);

        JavaPairRDD<ImmutableBytesWritable, Result> logsRDD = sparkContext.newAPIHadoopRDD(hbConfig,
                TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

        JavaPairRDD<ImmutableBytesWritable, Result> logFilterRDD = logsRDD.filter(element -> {
            List<Cell> listCells = element._2.listCells();
            boolean flat = false;
            if (listCells.size() > 0) {
                for (Cell cell : listCells) {
                    if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals("content")) {
                        String value = Bytes.toString(CellUtil.cloneValue(cell));
                        for (String key : KEY_WORD) {
                            if (value.toLowerCase().contains(key)) {
                                flat = true;
                            }
                        }

                    }
                }
            }
            return flat;
        });

//        JavaRDD<Map<String, String>> outputRDD = logFilterRDD.map(element -> {
//            String uID = Bytes.toString(element._1.get()).split("\\|")[1];
//            List<Cell> listCells = element._2.listCells();
//            StringBuilder content = new StringBuilder();
//            for (Cell cell : listCells) {
//                if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals("content")) {
//                    content.append(Bytes.toString(CellUtil.cloneValue(cell)));
//                }
//            }
//            return ImmutableMap.of("uID", uID, "content", content.toString());
//        });
//
//        JavaEsSpark.saveToEs(outputRDD, ELASTIC_INDEX + "/" + ELASTIC_TYPE);

        List<Map<String, String>> outputMap = logFilterRDD.flatMap(element -> {
            String uID = Bytes.toString(element._1.get()).split("\\|")[1];
            List<Cell> listCells = element._2.listCells();

            Map<String, String> json = new HashMap<>();
            json.put("uID", uID);
            for (Cell cell : listCells) {
                if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals("content")) {
                    String value = Bytes.toString(CellUtil.cloneValue(cell));
                    for (String key : KEY_WORD) {
                        if (value.toLowerCase().contains(key)) {
                            json.put("key_word", key);
                        }
                    }

                }
            }
            return Collections.singletonList(json).iterator();
        }).take(100);


//        for (Tuple2<ImmutableBytesWritable, Result> element : listUID) {
//            String uID = Bytes.toString(element._1.get()).split("\\|")[1];
//            List<Cell> listCells = element._2.listCells();
//
//            for (Cell cell : listCells) {
//                if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals("content")) {
//                    Map<String, Object> json = new HashMap<>();
//                    json.put("uid", uID);
//                    json.put("content", Bytes.toString(CellUtil.cloneValue(cell)));
//                    IndexResponse response = client.prepareIndex( ELASTIC_INDEX, index_type.toString())
//                            .setSource(json)
//                            .get();
//                    System.out.println(response.status());
//                }
//            }
//        }

        for (Map<String, String> doc : outputMap) {
            IndexResponse response = client.prepareIndex(ELASTIC_INDEX, ELASTIC_TYPE)
                    .setSource(doc)
                    .get();
            System.out.println(response.status());

        }

        closeSpark();
        closeElastic();
    }
}

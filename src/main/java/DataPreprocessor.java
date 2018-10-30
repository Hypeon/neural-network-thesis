import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.datavec.api.records.reader.RecordReader;
import org.datavec.api.records.reader.impl.csv.CSVRecordReader;
import org.datavec.api.transform.TransformProcess;
import org.datavec.api.transform.schema.Schema;
import org.datavec.api.transform.transform.time.DeriveColumnsFromTimeTransform;
import org.datavec.api.writable.Writable;
import org.datavec.spark.transform.SparkTransformExecutor;
import org.datavec.spark.transform.misc.StringToWritablesFunction;
import org.datavec.spark.transform.misc.WritablesToStringFunction;
import org.joda.time.DateTimeFieldType;
import org.joda.time.DateTimeZone;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataPreprocessor {
    public static void main(String[] args) {

        processData();

    }

    private static void processData() {
        System.out.println("------ Data processing STARTED ------");
        long startTime = System.currentTimeMillis();

        Map<String, String> mapping = new HashMap<>();
        mapping.put("\"", "");

        Schema inputDataSchema = getBasicSchema();
        TransformProcess tp = new TransformProcess.Builder(inputDataSchema)
            .removeColumns("Price","Open","High","Low","Vol.")
            .replaceStringTransform("DateString", mapping )
            .stringToTimeTransform("DateString", "MM dd yyyy", DateTimeZone.UTC)
            .renameColumn("DateString", "MsSince1970")
            .transform(new DeriveColumnsFromTimeTransform.Builder("MsSince1970")
                            .addIntegerDerivedColumn("Day", DateTimeFieldType.dayOfMonth())
                            .addIntegerDerivedColumn("Month", DateTimeFieldType.monthOfYear())
                            .addIntegerDerivedColumn("Year", DateTimeFieldType.year())
                            .build())
            .removeColumns("MsSince1970")
            .build();

        Schema outputSchema = tp.getFinalSchema();
        System.out.println("Schema after transforming data:");
        System.out.println(outputSchema);

        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("BTC/USD price prediction");

        JavaSparkContext sc = new JavaSparkContext(conf);

        String path = "./src/main/resources/BTC_USD_train.csv";
        JavaRDD<String> stringData = sc.textFile(path); //Resilient Distributed Dataset

        RecordReader rr = new CSVRecordReader();
        JavaRDD<List<Writable>> parsedInputData = stringData.map(new StringToWritablesFunction(rr));
        JavaRDD<List<Writable>> processedData = SparkTransformExecutor.execute(parsedInputData, tp);
        JavaRDD<String> processedAsString = processedData.map(new WritablesToStringFunction(","));
        processedAsString.saveAsTextFile("./src/main/resources/BTC_USD_processed_"
                                         + System.currentTimeMillis() + ".csv");

        System.out.println("------ Data processing COMPLETED ------");
        System.out.println("Time spent: " + (System.currentTimeMillis() - startTime) / 1000.0);
    }

    private static Schema getBasicSchema() {
        Schema inputDataSchema = new Schema.Builder()
            .addColumnString("DateString")
            .addColumnsDouble("Price","Open","High","Low","Vol.","Change %")
            .build();

        System.out.println("Input data schema:");
        System.out.println(inputDataSchema);
        return inputDataSchema;
    }
}

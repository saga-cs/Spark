

import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;

import scala.Tuple2;

public class MapandFilter {
    public static void main(String [] args){
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("Hello Spark");
        sparkConf.setMaster("local");
        JavaSparkContext context = new JavaSparkContext(sparkConf);

//        JavaRDD<Integer> numbersRDD = context.parallelize(Arrays.asList(1,2,3));
//
//        JavaRDD<Integer> squaresRDD = numbersRDD.map(n -> n*n);
//        System.out.println(squaresRDD.collect().toString());
//
//        JavaRDD<Integer> evensRDD = squaresRDD.filter(n-> n%2==0);
//        System.out.println(evensRDD.collect().toString());
//
//        JavaRDD<Integer> multipliedRDD = numbersRDD.flatMap(n ->Arrays.asList(n, n*2, n*3).iterator());
//        System.out.println(multipliedRDD.collect().toString());

//        JavaPairRDD<String, Integer> petsRDD = JavaPairRDD.fromJavaRDD(context.parallelize(Arrays.asList(
//                new Tuple2<String, Integer>("cat", 1),
//                new Tuple2<String, Integer>("Dog", 5),
//                new Tuple2<String, Integer>("cat", 3)
//        )));
//        System.out.println(petsRDD.collect().toString());
//
//        JavaPairRDD<String, Integer> agedPetsRDD = petsRDD.reduceByKey((v1,v2)-> Math.max(v1,v2));
//        System.out.println(agedPetsRDD.collect().toString());
//
//        JavaPairRDD<String, Iterable<Integer>> groupedPetsRDD = petsRDD.groupByKey();
//        System.out.println(groupedPetsRDD.collect().toString());

        JavaPairRDD<String,String> visitsRDD = JavaPairRDD.fromJavaRDD(context.parallelize(Arrays.asList(
                new Tuple2<String, String>("index.html", "1.2.3.4"),
                new Tuple2<String, String>("about.html", "3.4.5.6"),
                new Tuple2<String, String>("index.html", "1.3.3.1")
        )));

        JavaPairRDD<String, String> pageNamesRDD = JavaPairRDD.fromJavaRDD(context.parallelize(Arrays.asList(
                new Tuple2<String, String>("index.html", "Home"),
                new Tuple2<String, String>("about.html", "About"),
                new Tuple2<String,String>("index.html", "Welcome")
        )));

        JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> joinRDD = visitsRDD.cogroup(pageNamesRDD);
        System.out.println(joinRDD.collect().toString());

        context.close();
    }
}

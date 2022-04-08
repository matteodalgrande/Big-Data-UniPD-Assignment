import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;
import java.lang.Math;

public class HW1
{
    public static void main(String[] args) throws IOException
    {
        //checking parameters
        if (args.length != 3) {
            throw new IllegalArgumentException("USAGE: num_partitions num_products file_path");
        }

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // SPARK SETUP
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        SparkConf conf = new SparkConf(true).setAppName("HW1");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // INPUT READING
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        // Read number of partitions
        int K = Integer.parseInt(args[0]);

        // Read number of product to print
        int T = Integer.parseInt(args[1]);

        // Read input file and subdivide it into K random partitions
        JavaRDD<String> RawData = sc.textFile(args[2]).repartition(K).cache();

        //&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // SETTING GLOBAL VARIABLES
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        long numproduct;
        Random randomGenerator = new Random();
        numproduct = RawData.count();
        System.out.println("Number of products = " + numproduct);
        JavaPairRDD<String,Float> normalizedRatings;
        JavaPairRDD<String,Float> maxNormRatings;

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        //NORMALIZEDRATING
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        normalizedRatings= RawData.mapToPair((review) -> { //MAP PHASE (R1)
            String[] line = review.split(",");
            return new Tuple2<>(line[1],new Tuple2<String,Float>(line[0],Float.parseFloat(line[2])));
        }).groupByKey()  //REDUCE PHASE (R1)
          .flatMapToPair((user) -> { //MAP PHASE(R2)
              float AvgRating;
              float SumRatings=0;
              ArrayList<String> product = new ArrayList<>();
              ArrayList<Float>  ratings = new ArrayList<>();
              ArrayList<Tuple2<String,Float>> pairs = new ArrayList<>();
              for(Tuple2<String,Float> element : user._2())
              {
                  product.add(element._1()); // PRODUCT REVIEW BY USERx
                  ratings.add(element._2()); // RATING OF CORRESPONDING PRODUCT
              }
              for(int i=0;i<ratings.size();i++)              //&&&&&&&&&&&&&&&&&&&&&&&&&&
                  SumRatings= SumRatings + ratings.get(i);   //AVERAGE RATING CALCULATION
              AvgRating = SumRatings/(float)ratings.size();  //&&&&&&&&&&&&&&&&&&&&&&&&&&
              for(int i=0;i<product.size();i++)
                  pairs.add(new Tuple2<String,Float>(product.get(i),ratings.get(i)-AvgRating));
              return pairs.iterator();
          });

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // MAXNOMRRATINGS
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        maxNormRatings = normalizedRatings
                .mapToPair((product) -> {  //MAP PHASE (R1)
                    return new Tuple2<>(randomGenerator.nextInt(K),new Tuple2<String,Float>(product._1(),product._2())); // Assign a random integer [0,K] as key
        }).groupByKey() //REDUCE PHASE (R1)
        .flatMapToPair((element)->{ // MAP PHASE (R2)
            HashMap<String,Float> maximum = new HashMap<>();
            ArrayList<Tuple2<String,Float>> pairs = new ArrayList<>();
            for(Tuple2<String,Float> c : element._2())
                maximum.put(c._1(), Math.max(c._2(),maximum.getOrDefault(c._1(),  -6F))); // Found the MNR of Product "x" with key "y"
            for(Map.Entry<String,Float> e : maximum.entrySet())
                pairs.add(new Tuple2<>(e.getKey(),e.getValue()));
            return pairs.iterator();
        }).reduceByKey((pID1 , pID2) -> // REDUCE PHASE (R2)
            Math.max(pID1,pID2)); // Calculate the MNR for product x

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // PRINT TOP T MNR
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        List<Tuple2<String,Float>> print =  maxNormRatings.mapToPair(x->x.swap()).sortByKey(false).mapToPair(x->x.swap()).take(T); // Swap pID(key) with MNR(value), apply sortByKey and swap again
        for(int i=0;i<T;i++)
         System.out.println("Product "+print.get(i)._1()+" rating "+print.get(i)._2());

    }
}

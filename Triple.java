//package edu.drexel.cs461.preference;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.col;

/**
 * @author LIST YOUR NAMES HERE
 *         Frequent preference mining with Apache Spark SQL.
 */
public final class Triple {

    private static JavaSparkContext sparkContext;
    private static SQLContext sqlContext;

    /**
     * Set up Spark and SQL contexts.
     */
    private static void init(String master, int numReducers) {

        Logger.getRootLogger().setLevel(Level.OFF);

        SparkConf sparkConf = new SparkConf().setAppName("Triple")
                .setMaster(master)
                .set("spark.sql.shuffle.partitions", "" + numReducers);

        sparkContext = new JavaSparkContext(sparkConf);
        sqlContext = new org.apache.spark.sql.SQLContext(sparkContext);
    }

    /**
     * @param inFileName
     * @return
     */
    private static DataFrame initPref(String inFileName) {

        // read in the transactions file
        JavaRDD<String> prefRDD = sparkContext.textFile(inFileName);

        // establish the schema: PREF (tid: string, item1: int, item2: int)
        List<StructField> fields = new ArrayList<StructField>();
        fields.add(DataTypes.createStructField("tid", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("item1", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("item2", DataTypes.IntegerType, true));
        StructType prefSchema = DataTypes.createStructType(fields);

        JavaRDD<Row> rowRDD = prefRDD.map(
                new Function<String, Row>() {
                    static final long serialVersionUID = 42L;

                    public Row call(String record) throws Exception {
                        String[] fields = record.split("\t");
                        return RowFactory.create(fields[0],
                                Integer.parseInt(fields[1].trim()),
                                Integer.parseInt(fields[2].trim()));
                    }
                });

        // create DataFrame from prefRDD, with the specified schema
        return sqlContext.createDataFrame(rowRDD, prefSchema);
    }

    private static void saveOutput(DataFrame df, String outDir, String outFile) throws IOException {

        File outF = new File(outDir);
        outF.mkdirs();
        BufferedWriter outFP = new BufferedWriter(new FileWriter(outDir + "/" + outFile));

        List<Row> rows = df.toJavaRDD().collect();
        for (Row r : rows) {
            outFP.write(r.toString() + "\n");
        }

        outFP.close();
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 6) {
            System.err.println("Usage: Triple <inFile> <p_support> <t_support> <outDir> <numReducers> <master>");
            System.exit(1);
        }

        String inFileName = args[0].trim();
        double p_thresh = Double.parseDouble(args[1].trim());
        double t_thresh = Double.parseDouble(args[2].trim());
        String outDirName = args[3].trim();

        int numReducers = Integer.parseInt(args[4].trim());
        String master = args[5].trim();

        Triple.init(master, numReducers);
        Logger.getRootLogger().setLevel(Level.OFF);

        DataFrame pref = Triple.initPref(inFileName);

        DataFrame asdf = pref.groupBy("tid").count();


        //Looks for tIDs that are more than 2, if not we can throw them out
        List<Row> rows = asdf.toJavaRDD().collect();
        ArrayList<String> transactions = new ArrayList<String>();

        for(Row row: rows){
            Long test = row.getLong(1);
            if(test >= 2) {
                transactions.add(row.getString(0));
            }
        }
        System.out.println(transactions);
        System.out.println(rows);

        DataFrame aPairs = null;
        DataFrame vPairs = null;
        DataFrame lPairs = null;
        DataFrame allPairs = null;
        ArrayList<DataFrame> trans = new ArrayList<DataFrame>();
        for(String transaction : transactions) {
            DataFrame qwer = pref.select("tid", "item1", "item2").where(col("tid").equalTo(transaction));
            trans.add(qwer);

            aPairs = mergeFrames(aPairs, aPair(qwer));
            //vPairs = mergeFrames(vPairs, vPair(qwer));
            //lPairs = mergeFrames(lPairs, lPair(qwer));
        }

        //allPairs = mergeFrames(allPairs, aPairs);
        //allPairs = mergeFrames(allPairs, vPairs);

        DataFrame Triples = null;

        //lTriples
        DataFrame tempTriple = pref.select("item1", "item2")
                .join(pref.select(pref.col("item1").as("temp_item1")
                ,pref.col("item2").as("item3"))
                ,col("item2").equalTo(col("temp_item1")).and(col("item2").$less(col("item3"))));
        tempTriple = tempTriple.select("item1", "item2", "item3");
        tempTriple = tempTriple.filter(tempTriple.col("item1").notEqual(tempTriple.col("item3")));

        //lTriples
        DataFrame tempTriple1 = pref.select("item1", "item2");

        System.out.println("pref init");
        pref.show();

        System.out.println("Triple init");
        tempTriple.show();



        //v triples
        DataFrame tempTriple2 = pref.select("item1", "item2")
                .join(pref.select(pref.col("item2").as("temp_item2")
                ,pref.col("item1").as("item3"))
                ,col("item2").equalTo(col("temp_item2")).and(col("item2").$less(col("item3"))));
        tempTriple2 = tempTriple2.select("item1", "item2", "item3");
        tempTriple2 = tempTriple2.filter(tempTriple2.col("item1").notEqual(tempTriple2.col("item3")));

        System.out.println("vTriples");
        tempTriple2.show();

        //a triples






        long pairThreshold = (long)Math.floor(pref.select("tid").distinct().count() * p_thresh);
        long tripleThreshold = (long)Math.floor(pref.select("tid").distinct().count() * t_thresh);
        System.out.println("PairThreshold = " + pairThreshold + " TripleThreshold = " + tripleThreshold);


        DataFrame frequentPairs = pref.groupBy("item1", "item2").count();
        frequentPairs = frequentPairs.filter(frequentPairs.col("count").$greater$eq(pairThreshold));

        System.out.println("frequent Pairs");
        frequentPairs.show();

        DataFrame candidateTriples = frequentPairs.select("item1", "item2")
                .join(frequentPairs.select(frequentPairs.col("item1").as("temp_item1")
                        ,frequentPairs.col("item2").as("item3"))
                        ,col("item2").equalTo(col("temp_item1"))
                                .and(col("item2").$less(col("item3"))));

        candidateTriples.show();

        candidateTriples = candidateTriples.select("item1", "item2", "item3");

        candidateTriples.show();
        DataFrame frequentTriples = candidateTriples.groupBy("item1", "item2", "item3").count();

        frequentTriples.show();
        frequentTriples = frequentTriples.filter(frequentTriples.col("count").$greater$eq(tripleThreshold));

        frequentTriples.show();


        DataFrame lTriples = pref;
        DataFrame vTriples = pref;
        DataFrame aTriples = pref;


        try {
            Triple.saveOutput(lTriples, outDirName + "/" + t_thresh, "L");
        } catch (IOException ioe) {
            System.out.println("Cound not output L-Triples " + ioe.toString());
        }

        try {
            Triple.saveOutput(vTriples, outDirName + "/" + t_thresh, "V");
        } catch (IOException ioe) {
            System.out.println("Cound not output V-Triples " + ioe.toString());
        }

        try {
            Triple.saveOutput(aTriples, outDirName + "/" + t_thresh, "A");
        } catch (IOException ioe) {
            System.out.println("Cound not output A-Triples " + ioe.toString());
        }

        System.out.println("Done");
        sparkContext.stop();

    }
    
    public static DataFrame mergeFrames(DataFrame d1, DataFrame d2) {
    	if(d1 != null && d2 != null) {
    		return d1.unionAll(d2);
    	} else if(d1 != null) {
    		return d1;
    	} else {
    		return d2;
    	}
    }
    
    public static DataFrame aPair(DataFrame d) {
    	//Apairs logic
    	//Return the Apairs
    	//System.out.println("A Triples");
        DataFrame aaTriples = d.select("item1", "item2")
        		.join(d.select(d.col("item1").as("temp_item1"), d.col("item2").as("item3"))
        		,col("item1").equalTo(col("temp_item1"))
        		.and(col("item2").notEqual(col("item3"))
        		.and(col("item2").$less(col("item3")))));
        aaTriples = aaTriples.select(aaTriples.col("item2").as("item1"), aaTriples.col("temp_item1").as("item2"), aaTriples.col("item3"));
        
        //aaTriples.show();
        //System.out.println("---------------------------------------");
    	return aaTriples;
    }
}

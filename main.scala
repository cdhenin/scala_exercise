import org.apache.spark.sql._
import org.apache.hadoop.fs._;

val spark = SparkSession.builder.master("local[*]").appName("EarlyBirds").getOrCreate()
 
/**
** get input.csv an attribute columns names
**/ 
val dfInput = spark.read.csv("input/input.csv")
val columns_name = Seq("userId", "itemId", "rating", "timestamp")
val dfInputWithColumnsName = dfInput.toDF(columns_name: _*)

// dfInputWithColumnsName.select("userId").distinct().count() // res1: Long = 2197938                                                            
// dfInputWithColumnsName.select("itemId").distinct().count() // res2: Long = 316999

/**
** Create dfLookupUser and dfLookupProduct : generation of consecutive id
**/ 
val dfLookupUser = dfInputWithColumnsName.select("userId").distinct().withColumn("userIdAsInteger", monotonically_increasing_id.cast("int"))
val dfLookupProduct = dfInputWithColumnsName.select("itemId").distinct().withColumn("itemIdAsInteger", monotonically_increasing_id.cast("int"))

/**
** Add userIdAsInteger and itemIdAsInteger to the dataframe from input.csv
**/ 
val dfWithUserIdAsInteger = dfInputWithColumnsName.join(dfLookupUser, dfLookupUser("userId") === dfInputWithColumnsName("userId"))
val dfWithUserItemIdAsInteger = dfWithUserIdAsInteger.join(dfLookupProduct, dfLookupProduct("itemId") === dfWithUserIdAsInteger("itemId"))

val dfPreAggregation = dfWithUserItemIdAsInteger.select("userIdAsInteger", "itemIdAsInteger", "rating", "timestamp")

// dfPreAggregation.select("userIdAsInteger").distinct().count() // res3: Long = 2197938                                                            
// dfPreAggregation.select("itemIdAsInteger").distinct().count() // res4: Long = 316999

/**
** Add penality column = nb days betweem max timestamp and timestamp of the row
**/ 
val stringMaxTimeStamp = dfPreAggregation.select(max("timestamp")).first.getString(0) // result = 1477353599713
val dfPreAggregationWithPenality = dfPreAggregation.withColumn("penality", datediff(lit(stringMaxTimeStamp).cast("long").cast("int").cast("timestamp"), $"timestamp".cast("long").cast("int").cast("timestamp")))

/**
** Apply penality to rating and filter to keep only the ratings > 0.01
**/ 
val dfWithRatingPenalized = dfPreAggregationWithPenality.withColumn("ratingPenalized", $"rating" * ($"penality" * 0.95))
// dfWithRatingPenalized.count() // 11922520
val dfWithRatingPenalizedFiltered = dfWithRatingPenalized.filter($"ratingPenalized" > 0.01)
// dfWithRatingPenalizedFiltered.count() // 11922442

/**
** Group by userIdAsInteger and itemIdAsInteger and compute ratingSum column 
**/ 
val dfAggRatings = dfWithRatingPenalizedFiltered.groupBy("userIdAsInteger", "itemIdAsInteger").agg(sum($"ratingPenalized").cast("float").as("ratingSum"))
// dfAggRatings.count() // res1: Long = 4456693                                                           

// dfAggRatings.printSchema()
/* root 
	|-- userIdAsInteger: integer (nullable = false)
	|-- itemIdAsInteger: integer (nullable = false)
	|-- ratingSum: float (nullable = true) */

// dfLookupUser.printSchema()
/* root
 	|-- userId: string (nullable = true)
 	|-- userIdAsInteger: integer (nullable = false) */

// dfLookupProduct.printSchema()
/* root
 	|-- itemId: string (nullable = true)
 	|-- itemIdAsInteger: integer (nullable = false) */

/**
** Save dataframes into CSV files (with coalesce(1) to have only one CSV,
** To use only on standalone mode) 
**/ 
dfLookupUser.coalesce(1).write.option("header", "true").csv("output/lookup_user")
dfLookupProduct.coalesce(1).write.option("header", "true").csv("output/lookup_product")
dfAggRatings.coalesce(1).write.option("header", "true").csv("output/agg_ratings")

/**
** Rename output files to have lookup_product.csv, lookup_user.csv and agg_ratings.csv
** To use only on standalone mode  
**/
val fs = FileSystem.get(sc.hadoopConfiguration);
fs.rename(new Path("output/agg_ratings/" + fs.globStatus(new Path("output/agg_ratings/part*"))(0).getPath().getName()), new Path("output/agg_ratings/agg_ratings.csv"));
fs.rename(new Path("output/lookup_product/" + fs.globStatus(new Path("output/lookup_product/part*"))(0).getPath().getName()), new Path("output/lookup_product/lookup_product.csv"));
fs.rename(new Path("output/lookup_user/" + fs.globStatus(new Path("output/lookup_user/part*"))(0).getPath().getName()), new Path("output/lookup_user/lookup_user.csv"));


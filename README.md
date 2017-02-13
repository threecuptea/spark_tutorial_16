### spark_tutorial_16 is a showcase how different that Spark 1.6 is compared with spark 2. It includes 2 scala objects that I also collected in spark_tutorial_2: SparkZipExample , MovieLensALSMongo writen in Spark 1.6.
#### To summarize the difference:
    1. Spark 2 consolidates functional operations of RDD as well as relational operation of DataFrame into Dataset 
       so that we can mix funtional operations like map, filter and relational operations like select, groupBy, agg 
       together in one statement.  In Spark 1.6  Dataset is very limited.  We better stick to DataFrame and use RDD 
       occasionally.
       
    2. Spark ports lots of popular opertions into DataFrame or Column. For example,  DataFrame has filter operation 
       too and we can filter using isin, like, rlike, contains, isNotNull etc. for text Column and operators, isNaN & 
       between for numeric Column.
             
    3. When we are unable to find any DataFrame or Column method meet our needs, we have to convert it to rdd, apply
       operation applicable then convert it back to DataFrame.  That's where thing get very hairy.   For example,
       in MovieLensALSMongo16, I need to create unrated movie DF.  I have to exclude those movieId that appears in
       personRatings.  The RDD filter method using Seq.contains fit our needs.   We can use DataFrame.rdd method to 
       easily convert DataFrame to RDD.  However, it is not straight forward to convert RDD back to DataFrame.  
       We cannot use RDD.toDF method.  We cannot use sparkContext.createDataFrame either.  Both require schema info 
       (type etc).  We don't want to build StructType using StructField one by one.  I end up using pattern match.  
       Here are codes excerpt:
        
            val unratedRdd = allDS.select($"movie_id").distinct().rdd.filter(r => !pMovieIds.contains(r.getAs[Int](0)))
            val unratedDF =  unratedRdd.map({
              case Row(val1: Int) => MongoMovieId(val1)
            }).toDF()
        
       I have to create a case class MongoMovieId just for this purpose.  Luckily, I can replace 
         isNaN method of RDD.filter with the one of Column in DataFrame.filter like the followings:
         
            val rdd = model.transform(valDS).rdd.filter(r => !r.getAs[Float]("prediction").isNaN) 
            val prediction = model.transform(valDS).filter(!$"prediction".isNaN)
         
       otherwise, I need 2 more conversions of DataFrame to RDD and RDD to DataFrame.
         
    4. A Dataset is a strongly typed collection of domain-specific objects.
      
       In Spark 2, We can access a field of Dataset[Zips] with its field name so that we can filter like
        
            zipDS.filter(_.pop > 40000).select(...
            
       which will be checked on compile time or we can also filter using DataFrame Column like
        
            zipDF.filter($"pop" > 40000).select(...
            
       which won't be checked until the run-time.  Notice that we use functional operation 'filter' and relational
       operation 'select'in one statement in the former.
          
       In Spark 1.6, we can only use latter which uses purely relational DataFrame operation.
         
         
        
      
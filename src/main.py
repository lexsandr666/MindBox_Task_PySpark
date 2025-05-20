from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def get_product_category_pairs(products_df, categories_df, productCategories_df):
    return (
        products_df.alias("p")
            .join(productCategories_df.alias("pc"), col("p.id") == col("pc.product_id"), "left")
            .join(categories_df.alias("c"), col("pc.category_id") == col("c.id"), "left")
            .where(col("p.id").isNotNull())
            .select(
                col("p.name").alias("product_name"),
                col("c.name").alias("category_name")
            )
    )

if __name__ == "__main__":
    spark = SparkSession.builder.appName("ProductCategoryApp").getOrCreate()
    products_df   = spark.read.option("multiline", True).json("data/products.json")
    categories_df = spark.read.option("multiline", True).json("data/categories.json")
    productCategories_df   = spark.read.option("multiline", True).json("data/productCategories.json")
    result = get_product_category_pairs(products_df, categories_df, productCategories_df)

    result.show(truncate=False)
    spark.stop()
import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import split,trim


def main():
    # Initialize the config and read parameter file ('/parameters/config.json')
    config = CONFIG

    # Create Sprak Session
    spark = SparkSession.builder.appName('coding_challenge').getOrCreate()

    # Read the files from input directory which is provided as part of config.
    # Spark-csv was an external library by databricks until SPARK 2.0, After 2.0 csv api comes as native.
    # Have set inferschema to true in this case. We can also specify .schema(schema) i.e StructType of the CSV file
    input_data = spark.read.\
        format(config['file_type']).\
        option("header",config['header']).\
        option("inferSchema", config['infer_schema']).\
        load(config['input_directory'])

    # Splitting column species into two in order to extract the tree name
    split_species_col = split(input_data['species'], '::')

    # Create a new column called 'tree_name' based on the split performed before
    data_with_species = input_data.withColumn('tree_name', split_species_col.getItem(1))

    # To find the trees which are most common in San Fran Streets
    most_common_street_trees = get_most_common_trees(data_with_species)

    # Count of cherry plum trees which are dpw maintained
    cherry_plum_dpw_maintained_count = get_cherry_plum_count(data_with_species)

    # Count of banyan fig trees which has permit number
    banyan_fig_with_permit_count = get_banyan_fig_count(data_with_species)

    # Splitting the address column in order to get the street name to determine area with more trees
    split_address_col  = split(input_data['address'], ' ')

    # Extract street name as a new column
    data_with_street = input_data.withColumn('street_name', split_address_col.getItem(1))

    # Group the data set by street name to get the street with more number of trees
    street_with_more_trees = get_street_with_more_trees(data_with_street)

    # Group the dataset by site_order to get the site with more number of trees.
    site_with_more_trees = get_tree_count_by_site(input_data)

    most_common_street_trees.show()
    print("Cherry Plum DPW Maintained Count :" + str(cherry_plum_dpw_maintained_count))
    print("Banyan Fig with Permit Number Count :" + str(banyan_fig_with_permit_count))
    street_with_more_trees.show()
    site_with_more_trees.show()

    # Before writing repartitioning to 1 partition inorder to write the output as single file
    # coalesce can also be used, I choose repartition, ideally coalesce is better option when down sizing partitions
    most_common_street_trees.repartition(1).write.csv(config['output_directory'] + "Answer2/")
    street_with_more_trees.repartition(1).write.csv(config['output_directory'] + "Answer1a")
    site_with_more_trees.repartition(1).write.csv(config['output_directory'] + "Answer1b")


# After splitting the species, group the data set by tree_name and get the count for each tree and sort in descending
def get_most_common_trees(data_with_species):

    return data_with_species.\
        groupBy('tree_name').count().\
        withColumnRenamed("count","count_by_tree").\
        orderBy("count_by_tree", ascending=False)

# Trim the tree_name column with leading and trailing space, filter 'cherry plum' again filter 'DPW Maintained'
def get_cherry_plum_count(data_with_species):

    return data_with_species.\
        filter(trim(data_with_species['tree_name']) == 'Cherry Plum').\
        filter( data_with_species['legal_status'] == 'DPW Maintained').count()


# Trim the tree_name column with leading and trailing space, filter 'Banyan Fig' again filter ones with 'Permit number'
def get_banyan_fig_count(data_with_species):

    return data_with_species.\
        filter(trim(data_with_species['tree_name']) == 'Banyan Fig').\
        filter( data_with_species['permit_notes'].like('%Permit Number%')).count()


# "Area that is covered" is bit confusing, hence considered Site and street as two options

# After splitting the street name, group by street name and count and sort by the count
def get_street_with_more_trees(data_with_street):

    return data_with_street.\
        groupBy(data_with_street['street_name']).\
        count().withColumnRenamed("count","count_by_street")\
        .orderBy("count_by_street", ascending=False)


# Group by site_order and count and sort
def get_tree_count_by_site(input_data):

    return input_data.\
        groupBy('site_order').count()\
        .withColumnRenamed("count", "count_by_site").\
        orderBy("count_by_site", ascending=False)


# This method is to read the config file and handle any run time exceptions
def get_config():
    config_file= None
    path = os.path.dirname(os.path.realpath(__file__))
    try:
        config_file = open(file="{}/parameters/config.json".format(path), mode="r")
        config_str = config_file.read()
        config_json = json.loads(config_str)
        return config_json
    finally:
        if (config_file!=None):
            config_file.close()

CONFIG = get_config()

if __name__ == "__main__":
    main()



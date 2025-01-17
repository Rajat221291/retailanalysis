# Read config file

import configparser
from pyspark import SparkConf

# loading application configs in python dictionary
def get_app_config(env):
    """
    Returns a dictionary containing the configuration settings for a specific environment.

    Parameters:
    env (str): The environment for which the configuration settings are to be retrieved.

    Returns:
    dict: A dictionary containing the configuration settings for the specified environment.
    """
    config = configparser.ConfigParser()
    config.read('configs/application.conf')
    app_conf={}
    for (key, value) in config.items(env):
        app_conf[key]=value
    return app_conf
    
# loading the pyspark configs and creating a spark conf object
def get_pyspark_config(env):
    """
    Returns a SparkConf object containing the configuration settings for a specific environment.

    Parameters:
    env (str): The environment for which the configuration settings are to be retrieved.

    Returns:
    SparkConf: A SparkConf object containing the configuration settings for the specified environment.
    """
    config = configparser.ConfigParser()
    config.read('configs/pyspark.conf')
    pyspark_conf = SparkConf()
    for (key, value) in config.items(env):
        pyspark_conf.set(key, value)
    return pyspark_conf
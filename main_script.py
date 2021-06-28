from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
from random import randrange
import json
from py2neo import Graph
from py2neo import Node
from py2neo import Relationship

#---arguments for dag initiation
default_args = {                
    "owner" : "airflow",
    "depends_on_past" : False,
    "start_date" : days_ago(30),
    "email" : ["dongeo77@gmail.com"],
    "email_on_failure" : False,
    "email_on_retry" : False,
    "retries" : 1,
    "retry_delay" : timedelta(minutes = 1)
}

#---initiate a DA graph
dag_for_app_usage = DAG(
    "data_of_app_usage",
    default_args = default_args,
    description = "complete history of mobile app usage in neo4j database",
    schedule_interval = timedelta(days = 1)
)


##---collect data and store as json files
def collect_app_data(**kwargs):
    global total_time  # this variable keeps total duration under 480 minutes
    total_time = 0

    #--function to get a random time duration of app usage
    def get_duration():
        while 1:
            global total_time
            number = randrange(0,180)
            if number + total_time < 480:
                total_time += number
                return number

    #--get time dynamically from when dag is initiated
    date_object = kwargs["execution_date"]
    date = str(date_object)
    #--first name and last name are divided for ease of use
    users = ["vinit","guilermo","christian","elly","don"]
    last_name = "@tribes.ai"

    #-execute for every person
    for first_name in users:
        full_name = first_name+last_name #concat names         
        dictionary = {                   #create a dictonary 
            "user_id":full_name,
            "usages_date": date[0:10],   #slice to avoid unnecessary jargon from date.
            "device": {
                "os":"ios",
                "brand": "apple"
            },
            "usages": [
                {
                    "app_name": "slack",
                    "minutes_used": get_duration(), #getting random duration
                    "app_category": "communication"
                },
                {
                    "app_name": "gmail",
                    "minutes_used": get_duration(),
                    "app_category": "communication"
                },
                {
                    "app_name": "jira",
                    "minutes_used": get_duration(),
                    "app_category": "task_management"
                },
                {
                    "app_name": "google drive",
                    "minutes_used": get_duration(),
                    "app_category": "file_management"
                },
                {
                    "app_name": "chrome",
                    "minutes_used": get_duration(),
                    "app_category": "web_browser"
                },
                {
                    "app_name": "spotify",
                    "minutes_used": get_duration(),
                    "app_category": "entertainment_music"
                }
            ]
        }
    
        json_object = json.dumps(dictionary,indent = 2)     #convert dictionary into json object
        with open(first_name+".json","a") as output_file:   #complete history of usage details are stored
            output_file.write("\n")                         #to put newline between two objects                
            output_file.write(json_object)                  #files are stored locally
        with open(first_name+"_latest.json","w") as output_file1: 
            output_file1.write(json_object)                 #only the latest usage details are stored
        total_time = 0                                      #reset total time

#-------------------

#----Load data into the database
def load_into_neo4j():
    graph = Graph("neo4j://0.0.0.0:7687", auth=("neo4j", "password")) #graph instance
    graph.delete_all()
    users = ["vinit","guilermo","christian","elly","don"]
    for first_name in users:
        increment = 0 #for accessing usages list
        file = open(first_name+"_latest.json","r")
        data = json.load(file)
        #--variables
        user_id = data["user_id"]
        date_of_use = data["usages_date"]
        operating_system = data["device"]["os"]
        device_brand = data["device"]["brand"]
        app_name = data["usages"][increment]["app_name"]
        app_category = data["usages"][increment]["app_category"]
        minutes_used = data["usages"][increment]["minutes_used"]
        increment += 1
        #-------
        #--creating nodes
        user = Node("User", name = "User", IdMaster = user_id)
        app = Node("App", name = "App", IdMaster = app_name, AppCategory = app_category)
        device = Node("Device", name = "Device", IdMaster = operating_system)
        brand = Node("Brand", name = "Brand", IdMaster = device_brand)
        
        #---getting current time from neo4j
        realtime = """
            RETURN time.realtime() 
        """
        time = None         
        raw_time_object = graph.run(realtime) 
        for item in raw_time_object:
            time = item[0]
        
        #--commiting creation command
        graph.create(user)
        graph.create(app)
        graph.create(device)
        graph.create(brand)

        #--creating relationship between nodes
        graph.create(Relationship(user, "USED", app, **{"TimeCreated":time,"TimeEvent":date_of_use, "UsageMinutes":minutes_used}))
        graph.create(Relationship(app, "ON", device, **{"TimeCreated":time}))
        graph.create(Relationship(device, "OFF", brand, **{"TimeCreated":time}))

#--task 1 -> collect data
run1 = PythonOperator(
    task_id = "collect_app_data",
    python_callable = collect_app_data,
    dag = dag_for_app_usage
)
#--task 2 -> load collected data
run2 = PythonOperator(
    task_id = "load_collected_data",
    python_callable = load_into_neo4j,
    dag = dag_for_app_usage
)

#--dependencies
run1>>run2  # more like what order to run tasks
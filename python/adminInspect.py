#python adminInspect.py.py
from confluent_kafka.admin import AdminClient
if __name__ == "__main__":
    ac = AdminClient({"bootstrap.servers": "[::1]:9092"})
    spc = "    "
    
    cm = ac.list_topics()
    print("----------------------------------------")
    print(f"  ClusterMetadata: {type(cm)}")
    print("----------------------------------------")
    print(f"{spc}cluster_id: {type(cm.cluster_id).__name__} = {cm.cluster_id}")
    print(f"{spc}controller_id: {type(cm.controller_id).__name__} = {cm.controller_id}")
    print(f"{spc}brokers: {type(cm.brokers).__name__} = {cm.brokers}")
    print(f"{spc}orig_broker_id: {type(cm.orig_broker_id).__name__} = {cm.orig_broker_id}")
    print(f"{spc}orig_broker_name: {type(cm.orig_broker_name).__name__} = {cm.orig_broker_name}")
    print(f"{spc}topics: {type(cm.topics).__name__} = ")
    for k, v in cm.topics.items():
        print(f"{spc}{spc}{k}")
    print("")



    topics = cm.topics
    tm = topics["test"]
    print("----------------------------------------")
    print(f"  TopicMetadata: {type(tm)}")
    print("----------------------------------------")
    print(f"{spc}topic: {type(tm.topic).__name__} = {tm.topic}")
    print(f"{spc}error: {type(tm.error).__name__} = {tm.error}")
    print(f"{spc}Number of partitions: {len(tm.partitions)}. Partition id's:")
    for k, v in tm.partitions.items():
        print(f"{spc}{spc}{k}")
    print(f"{spc}partitions: {type(tm.partitions).__name__} = {tm.partitions}")
    print("")



    p = tm.partitions[0]
    print("----------------------------------------")
    print(f"  PartitionMetadata: {type(p)}")
    print("----------------------------------------")
    print(f"{spc}id: {type(p.id).__name__} = {p.id}")
    print(f"{spc}leader: {type(p.leader).__name__} = {p.leader}")
    print(f"{spc}replicas: {type(p.replicas).__name__} = {p.replicas}")
    print(f"{spc}isrs: {type(p.isrs).__name__} = {p.isrs}")
    print(f"{spc}error: {type(p.error).__name__} = {p.error}")
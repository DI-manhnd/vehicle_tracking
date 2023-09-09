from kafka import KafkaConsumer, consumer
from time import sleep
import json
from azure.storage.blob import AppendBlobService
from datetime import datetime
import time
import json


class MessageConsumer:
    account_name = ""
    account_key = ""
    container_name = ""
    broker = ""
    topic = ""
    logger = None
    tmp_dict_for_objects = (
        {}
    )  # This include key = track_id, value = bool - if 0 mean in left of line, 1 = right of line
    objects_counter = {
        "bus": 0,
        "car": 0,
        "motobike": 0,
        "truck": 0,
        "train": 0,
    }  # This is counter for objects. If the object start from left of line to the right of line then it will plus 1
    line_crossing = []

    def __init__(self, config_path):
        # Read lines from file txt
        config_file = open(config_path, "r")
        while True:
            line = config_file.readline().strip("\n")
            if not line:
                break
            key = line.split("=")[0]
            value = line.replace(key + "=", "")
            if key == "account_name":
                self.account_name = value
            elif key == "account_key":
                self.account_key = value
            elif key == "container_name":
                self.container_name = value
            elif key == "broker":
                self.broker = value
            elif key == "topic":
                self.topic = value
            elif key == "line-crossing":
                self.line_crossing = value.split(";")
                self.line_crossing = [int(x) for x in self.line_crossing]

    # This function check if the ceneter of object is in the left side of line
    def is_left_of_line(self, object_bbox, line):
        object_center = [
            (object_bbox[0] + object_bbox[2]) / 2,
            (object_bbox[1] + object_bbox[3]) / 2,
        ]
        top_line_point = [line[0], line[1]]
        bot_line_point = [line[2], line[3]]
        return (
            object_center[1] > top_line_point[1]
            and object_center[1] < bot_line_point[1]
            and (
                (top_line_point[0] - object_center[0])
                * (bot_line_point[1] - object_center[1])
                - (top_line_point[1] - object_center[1])
                * (bot_line_point[0] - object_center[0])
                > 0
            )
        )

    def count_objects_by_message(self, message_json):
        frame_id = message_json["id"]
        timestamp = message_json["@timestamp"]
        objects = message_json["objects"]

        for object_string in objects:
            object_id, x1, y1, x2, y2, object_name = object_string.split("|")
            bbox_object = [float(x1), float(y1), float(x2), float(y2)]
            if object_id in self.tmp_dict_for_objects:
                # This logic check if object was exist in left side and now moved to the rights
                if self.tmp_dict_for_objects[
                    object_id
                ] == 0 and not self.is_left_of_line(bbox_object, self.line_crossing):
                    # We count the object and then remove the track_id of object from tmp dict
                    self.objects_counter[object_name] += 1
                    self.tmp_dict_for_objects.pop(object_id)
            else:
                if self.is_left_of_line(bbox_object, self.line_crossing):
                    # Make sure number of track_id in the same time not higher than 500 - not out of memory
                    if len(self.tmp_dict_for_objects) > 500:
                        first_key = next(iter(self.tmp_dict_for_objects))
                        self.tmp_dict_for_objects.pop(first_key)
                    self.tmp_dict_for_objects[object_id] = 0

    def append_data_to_blob(self, data):
        now = datetime.now()
        t = int(time.time() * 1000)
        current_time = now.strftime("%d_%m_%Y_%H_%M_%S_")
        name_to_save = current_time + str(t) + ".json"
        service = AppendBlobService(
            account_name=self.account_name, account_key=self.account_key
        )
        try:
            service.append_blob_from_text(
                container_name=self.container_name,
                blob_name=self.name_to_save,
                text=json.dumps(data),
            )
        except:
            service.create_blob(
                container_name=self.container_name, blob_name=name_to_save
            )
            service.append_blob_from_text(
                container_name=self.container_name,
                blob_name=name_to_save,
                text=json.dumps(data),
            )
        print(name_to_save, "Data Appended to Blob Successfully.")

    def activate_listener(self):
        consumer = KafkaConsumer(
            bootstrap_servers=self.broker,
            group_id="my-group",
            consumer_timeout_ms=60000,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            value_deserializer=lambda m: json.loads(m.decode("ascii")),
        )

        consumer.subscribe(self.topic)
        print("consumer is listening....")
        try:
            for message in consumer:
                # append_data_to_blob(message.value)
                self.count_objects_by_message(message.value)
                print(self.objects_counter)
                consumer.commit()
        except KeyboardInterrupt:
            print("Aborted by user...")
        finally:
            consumer.close()


consumer1 = MessageConsumer("config.txt")
consumer1.activate_listener()

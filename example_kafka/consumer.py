from kafka import KafkaConsumer, consumer
from time import sleep
import json
from azure.storage.blob import AppendBlobService
import datetime
import time
import json
from json import JSONEncoder


class DateTimeEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()

datetime_enc = DateTimeEncoder()

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
        "train": 0,
    }  # This is counter for objects. If the object start from left of line to the right of line then it will plus 1
    last_has_train = False
    count_confirm_no_train = 60
    line_crossing = []
    # This is checking time for sending to cloud
    send_time_interval = 0
    # first_send_after_start = 0
    is_send_to_cloud = False
    start_time = 0
    sent_count = 0
    diff_time = 0

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
            elif key == "send_time_interval":
                self.send_time_interval = int(value)
            elif key == "start_time":
                year, month, day, hour, minute = value.split(".")
                self.start_time = datetime.datetime(int(year), int(month), int(day), int(hour), int(minute))
            elif key == "diff_time":
                self.diff_time = int(value)

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

    def is_in_time(self, deepstream_message_time):
        date = deepstream_message_time[0:10]
        year, month, day = date.split("-")
        time = deepstream_message_time[11:16]
        hour, minute = time.split(":")
        current_time_message = datetime.datetime(int(year), int(month), int(day), int(hour), int(minute))
        current_time_message = current_time_message + datetime.timedelta(hours=self.diff_time)
        diff_t = (current_time_message - self.start_time).total_seconds() // 60
        if diff_t > 0:
            if diff_t < self.send_time_interval:
                return True
            else:
                self.start_time = self.start_time + datetime.timedelta(minutes=self.send_time_interval)
                self.is_send_to_cloud = True
                return False

    def count_objects_by_message(self, message_json):
        frame_id = message_json["id"]
        timestamp = message_json["@timestamp"]
        objects = message_json["objects"]

        if self.is_in_time(timestamp):
            have_train = False
            for object_string in objects:
                object_id, x1, y1, x2, y2, object_name = object_string.split("|")
                bbox_object = [float(x1), float(y1), float(x2), float(y2)]
                # Hackly logic for train, because only one train per time
                if object_name == "train":
                    have_train = True
                    if self.last_has_train == False:
                        self.objects_counter[object_name] += 1
                    continue
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
            if have_train == False and self.last_has_train == True:
                self.count_confirm_no_train -= 1
            if have_train == True:
                self.last_has_train = True
                self.count_confirm_no_train = 60

            if self.count_confirm_no_train == 0:
                self.last_has_train = False
                self.count_confirm_no_train = 60
        print(self.objects_counter)

    def append_data_to_blob(self):
        if (self.is_send_to_cloud):
            now = datetime.datetime.now()
            current_time = now.strftime("%d_%m_%Y_%H_%M_%S")
            name_to_save = current_time + ".json"
            data = {
                "bus" : str(self.objects_counter["bus"]),
                "car" : str(self.objects_counter["car"]),
                "motobike" : str(self.objects_counter["motobike"]),
                "train" : str(self.objects_counter["train"]),
                "time_stamp" : datetime.datetime.now()
            }

            data = datetime_enc.encode(data)

            # service = AppendBlobService(
            #     account_name=self.account_name, account_key=self.account_key
            # )
            # service.create_blob(
            #     container_name=self.container_name, blob_name=name_to_save
            # )
            # service.append_blob_from_text(
            #     container_name=self.container_name,
            #     blob_name=name_to_save,
            #     text=data,
            # )
            print(self.objects_counter)
            self.objects_counter = {
                "bus": 0,
                "car": 0,
                "motobike": 0,
                "train": 0,
            }  # Recount
            print("Current time: ", current_time, "Data Appended to Blob Successfully.")
            self.is_send_to_cloud = False
        # else :
            # print(self.objects_counter)

    def activate_listener(self):
        consumer = KafkaConsumer(
            bootstrap_servers=self.broker,
            group_id="my-group",
            consumer_timeout_ms=600000,
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
                self.append_data_to_blob()
                consumer.commit()
        except KeyboardInterrupt:
            print("Aborted by user...")
        finally:
            consumer.close()


consumer1 = MessageConsumer("config.txt")
consumer1.activate_listener()

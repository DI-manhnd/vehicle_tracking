from ultralytics import YOLO
import cv2 as cv
from datetime import datetime
import time

last_time = 0

model = YOLO("best.pt")
cap = cv.VideoCapture("/dev/video0")
count_bus_truck = 0
total = 0

if not cap.isOpened():
    print("Cannot open camera")
    exit()
while (total < 10000):
    ret, frame = cap.read()
    if not ret:
        print("Can't receive frame (stream end?). Exiting ...")
        break
    else:
        save = False
        have_truck_bus = False
        now = datetime.now()
        t = int(time.time() * 1000)
        current_time = now.strftime("%d_%m_%Y_%H_%M_%S_")
        name_to_save = "data/" + current_time + str(t)
        results = model(source=frame)
        str_to_save = ""
        for result in results:
            boxes = result.boxes
            for box in boxes:
                b_xywh = box.xywhn[0]
                class_ = int(box.cls.item())
                str_to_save += str(class_) + " " + str(b_xywh[0].item()) + " " + str(b_xywh[1].item()) + " " + str(b_xywh[2].item()) + " " + str(b_xywh[3].item()) + "\n"
                if (class_ == 0 or class_ == 3):
                    have_truck_bus = True
                    if (not save and t - last_time < 20000 and count_bus_truck % 50 == 0):
                        if (class_ == 0):
                            name_to_save += "bus"
                        if (class_ == 3):
                            name_to_save += "truck"
                        save = True
        if (have_truck_bus):
            count_bus_truck += 1

        if (t - last_time > 20000 or save):
            total += 1
            cv.imwrite(name_to_save + ".jpg", frame)
            f = open(name_to_save + ".txt", "a")
            f.write(str_to_save)
            f.close()
            last_time = t
cap.release()
cv.destroyAllWindows()

from ultralytics import YOLO
import numpy as np
from PIL import Image
from collections import Counter


class Detector:
    def __init__(self, weigh_path: str = "./weights/yolov8.pt"):
        self.model = YOLO(weigh_path)
        self.target_classes = ("person", "car", "motorcycle", "bus", "truck", "boat", "dog", "horse")
        self.names = self.model.names

    def predict(self, image: np.ndarray, *args, **kwargs) -> dict:
        labels = Counter()
        prediction = self.model.predict(image)
        bboxes = {}

        # there is an easier way
        for pred in prediction[0]:
            cls_label = self.names[int(pred.boxes.cls.numpy()[0])]
            labels[cls_label] += 1
            int_bbox = self.to_int_bbox(pred.boxes.xyxy.numpy()[0])
            if cls_label in bboxes:
                bboxes[cls_label].append(int_bbox)
            else:
                bboxes[cls_label] = [self.to_int_bbox(int_bbox)]

        return {"labels": labels, "bboxes": bboxes}

    def __call__(self, *args, **kwargs):
        return self.predict(*args, **kwargs)

    @staticmethod
    def to_int_bbox(bbox: np.array) -> np.array:
        return np.round(bbox).astype(int)


if __name__ == "__main__":
    img = np.array(Image.open("./weights/street.jpeg"))
    det = Detector("/home/gleb/PycharmProjects/couse/weights/yolov8n.pt")
    res = det(img)
    print(res)

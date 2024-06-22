from concurrent.futures import ThreadPoolExecutor

from ImageGathering.imageCapture import ImageCapture
from ImageGathering.Detector import Detector
import json


class Processor:
    def __init__(self, capture_interval=10):
        with open("./ImageGathering/configs/cameras.json", 'r') as f:
            self.cameras = json.load(f)
            print(self.cameras)
            self.capture_interval = 10


processor = Processor()
if __name__ == "__main__":
    with ThreadPoolExecutor(max_workers=len(processor.cameras)) as executor:
        futures = [
            executor.submit(ImageCapture.handle_camera, camera["place"], camera["video_id"], processor.capture_interval)
            for camera in processor.cameras
        ]

    try:
        for future in futures:
            future.result()
    except KeyboardInterrupt:
        print("Процесс был прерван пользователем")
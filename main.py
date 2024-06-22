import json
from concurrent.futures import ThreadPoolExecutor
from ImageGathering.imageCapture import ImageCapture

if __name__ == "__main__":
    with open('ImageGathering/config.json', 'r') as f:
        config = json.load(f)

    cameras = config["cameras"]
    capture_interval = config["capture_interval"]

    image_capture = ImageCapture("./ImageGathering/weights/yolov8n.pt")

    with ThreadPoolExecutor(max_workers=len(cameras)) as executor:
        futures = [
            executor.submit(image_capture.handle_camera, camera_name, camera_params, capture_interval)
            for camera_name, camera_params in cameras.items()
        ]

    try:
        for future in futures:
            future.result()
    except KeyboardInterrupt:
        print("Процесс был прерван пользователем")

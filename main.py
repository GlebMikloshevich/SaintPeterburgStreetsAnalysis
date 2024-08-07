from concurrent.futures import ThreadPoolExecutor
from ImageGathering.imageCapture import ImageCapture

from ImageGathering.db_module.db_commands import get_cameras

CAPTURE_INTERVAL = 1

if __name__ == "__main__":
    cameras = get_cameras()
    print("cameras", cameras)
    capture_interval = CAPTURE_INTERVAL

    image_capture = ImageCapture("./ImageGathering/weights/yolov8n.pt")

    with ThreadPoolExecutor(max_workers=len(cameras)) as executor:
        futures = [
            executor.submit(image_capture.handle_camera, camera_url, camera_params, capture_interval)
            for camera_url, camera_params in cameras.items()
        ]

    try:
        for future in futures:
            future.result()
    except KeyboardInterrupt:
        print("Процесс был прерван пользователем")

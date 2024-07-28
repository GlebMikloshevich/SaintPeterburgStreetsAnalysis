import cv2
import subprocess
import time
from datetime import datetime
import os
import json
from ImageGathering.Detector import Detector
from ImageGathering.db_module.db_commands import insert_result


class ImageCapture:
    def __init__(self, weight_path: str = "./weights/yolov8.pt"):
        self.detector = Detector(weight_path)

    def get_youtube_live_url(self, video_id):
        result = subprocess.run(
            ['yt-dlp', '-g', f'https://www.youtube.com/watch?v={video_id}'],
            capture_output=True,
            text=True
        )
        stream_url = result.stdout.strip()
        if not stream_url:
            raise RuntimeError(f"Не удалось получить URL потока для видео {video_id}")
        return stream_url

    def process_video_stream(self, camera_params, stream_url, capture_interval):
        cap = cv2.VideoCapture(stream_url)
        if not cap.isOpened():
            print(f"Не удалось открыть поток {stream_url} для камеры {camera_params['street']}")
            return

        last_capture_time = time.time()

        while cap.isOpened():
            ret, frame = cap.read()
            if not ret:
                break

            current_time = time.time()
            if current_time - last_capture_time >= capture_interval * 60:
                last_capture_time = current_time
                self.save_frame_and_process(camera_params, frame)

            if cv2.waitKey(1) & 0xFF == ord('q'):
                break

        cap.release()

    def convert_frame_to_bytes(self, frame):
        # Encode the frame as JPEG
        ret, buffer = cv2.imencode('.jpg', frame)
        if not ret:
            print(f"Failed to encode frame")
            return None

        # Convert the buffer to a byte array
        byte_array = buffer.tobytes()

        return byte_array

    def save_frame_and_process(self, camera_params, frame):
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        directory = f"data/{camera_params['street']}"
        if not os.path.exists(directory):
            os.makedirs(directory)
        filename = os.path.join(directory, f"{camera_params['street']}_{timestamp}.jpg")
        # cv2.imwrite(filename, frame) # was used for the debug
        byte_image = self.convert_frame_to_bytes(frame)
        byte_image = None  # TODO: move out
        print(f"Скриншот сохранен как {filename}")

        # Обработка кадра через YOLO
        results = self.detector.crop_predict(frame, camera_params["crop_list"])
        print(f"Результаты обработки для {camera_params['street']}:", results)

        # Преобразование numpy массивов в списки для сериализации в JSON
        results_serializable = {
            "timestamp": timestamp,
            "labels": {k: int(v) for k, v in results["labels"].items()},
            "bboxes": {k: [[int(coord) for coord in bbox] for bbox in v] for k, v in results["bboxes"].items()}
        }

        results_db = {
            "image": byte_image,
            "labels": {k: int(v) for k, v in results["labels"].items()},
            "bboxes": {k: [[int(coord) for coord in bbox] for bbox in v] for k, v in results["bboxes"].items()},
            "camera_id": camera_params["camera_id"]
        }

        result_id = insert_result(results_db)
        print(f"Inserted result with ID: {result_id}")

        # Путь к JSON-файлу
        results_filename = os.path.join(directory, f"{camera_params['street']}_results.json")

        # Чтение существующего JSON-файла, если он существует
        if os.path.exists(results_filename):
            with open(results_filename, 'r') as f:
                existing_results = json.load(f)
        else:
            existing_results = []

        # Добавление новых результатов
        existing_results.append(results_serializable)

        # Сохранение обновленных результатов в JSON-файл
        with open(results_filename, 'w') as f:
            json.dump(existing_results, f, indent=4)
        print(f"Результаты обработки сохранены как {results_filename}")

    def handle_camera(self, camera_url, camera_params, capture_interval):
        try:
            print(f"Получение URL потока для {camera_params['street']}...")
            stream_url = self.get_youtube_live_url(camera_url)

            print(f"Обработка видеопотока для {camera_params['street']}...")
            self.process_video_stream(camera_params, stream_url, capture_interval)
        except RuntimeError as e:
            print(e)

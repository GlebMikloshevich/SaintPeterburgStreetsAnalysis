import cv2
import subprocess
import time
from datetime import datetime
import os
import json
from Detector import Detector

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

    def process_video_stream(self, camera_name, stream_url, capture_interval):
        cap = cv2.VideoCapture(stream_url)
        if not cap.isOpened():
            print(f"Не удалось открыть поток {stream_url} для камеры {camera_name}")
            return

        last_capture_time = time.time()

        while cap.isOpened():
            ret, frame = cap.read()
            if not ret:
                break

            current_time = time.time()
            if current_time - last_capture_time >= capture_interval * 60:
                last_capture_time = current_time
                self.save_frame_and_process(camera_name, frame)

            if cv2.waitKey(1) & 0xFF == ord('q'):
                break

        cap.release()

    def save_frame_and_process(self, camera_name, frame):
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        directory = f"{camera_name}"
        if not os.path.exists(directory):
            os.makedirs(directory)
        filename = os.path.join(directory, f"{camera_name}_{timestamp}.jpg")
        cv2.imwrite(filename, frame)
        print(f"Скриншот сохранен как {filename}")

        # Обработка кадра через YOLO
        results = self.detector(frame)
        print(f"Результаты обработки для {camera_name}:", results)

        # Преобразование numpy массивов в списки для сериализации в JSON
        results_serializable = {
            "timestamp": timestamp,
            "labels": results["labels"],
            "bboxes": {k: [bbox.tolist() for bbox in v] for k, v in results["bboxes"].items()}
        }

        # Путь к JSON-файлу
        results_filename = os.path.join(directory, f"{camera_name}_results.json")

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

    def handle_camera(self, camera_name, video_id, capture_interval):
        try:
            print(f"Получение URL потока для {camera_name}...")
            stream_url = self.get_youtube_live_url(video_id)

            print(f"Обработка видеопотока для {camera_name}...")
            self.process_video_stream(camera_name, stream_url, capture_interval)
        except RuntimeError as e:
            print(e)

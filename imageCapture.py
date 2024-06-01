import cv2
import subprocess
import time
from datetime import datetime
import os
from concurrent.futures import ThreadPoolExecutor

def get_youtube_live_url(video_id):
    # Используем yt-dlp для получения прямого URL потока
    result = subprocess.run(
        ['yt-dlp', '-g', f'https://www.youtube.com/watch?v={video_id}'],
        capture_output=True,
        text=True
    )
    stream_url = result.stdout.strip()
    if not stream_url:
        raise RuntimeError(f"Не удалось получить URL потока для видео {video_id}")
    return stream_url

def process_video_stream(camera_name, stream_url, capture_interval):
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
            save_frame(camera_name, frame)

        # Проверка нажатия клавиши 'q' для выхода
        if cv2.waitKey(1) & 0xFF == ord('q'):
            break

    cap.release()

def save_frame(camera_name, frame):
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    directory = f"{camera_name}"
    if not os.path.exists(directory):
        os.makedirs(directory)
    filename = os.path.join(directory, f"{camera_name}_{timestamp}.jpg")
    cv2.imwrite(filename, frame)
    print(f"Скриншот сохранен как {filename}")

def handle_camera(camera_name, video_id, capture_interval):
    try:
        print(f"Получение URL потока для {camera_name}...")
        stream_url = get_youtube_live_url(video_id)

        print(f"Обработка видеопотока для {camera_name}...")
        process_video_stream(camera_name, stream_url, capture_interval)
    except RuntimeError as e:
        print(e)

if __name__ == "__main__":
    cameras = {
        'camera1': 'rmStl3GF2Ds',
        'camera2': 'ZlDohRExM-A'
    }
    capture_interval = 1  # Интервал захвата в минутах

    with ThreadPoolExecutor(max_workers=len(cameras)) as executor:
        futures = [
            executor.submit(handle_camera, camera_name, video_id, capture_interval)
            for camera_name, video_id in cameras.items()
        ]

    try:
        for future in futures:
            future.result()
    except KeyboardInterrupt:
        print("Процесс был прерван пользователем")

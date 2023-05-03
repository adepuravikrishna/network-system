import threading
import time
import random
import sqlite3
from queue import Queue

class Sensor:
    def __init__(self, name, queue, delay=5):
        self.name = name
        self.queue = queue
        self.delay = delay
        self._running = False
        self.thread = threading.Thread(target=self.run)

    def start(self):
        self._running = True
        self.thread.start()

    def stop(self):
        self._running = False
        self.thread.join()

    def run(self):
        while self._running:
            value = random.randint(-100, 100)
            message = (time.time(), self.name, value)
            self.queue.put(message)
            time.sleep(self.delay)

class Log:
    def __init__(self, queue, db_file):
        self.queue = queue
        self.db_file = db_file
        self._running = False
        self.thread = threading.Thread(target=self.run)

    def start(self):
        self._running = True
        self.thread.start()

    def stop(self):
        self._running = False
        self.thread.join()

    def run(self):
        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS logs
                     (id INTEGER PRIMARY KEY, timestamp REAL, sensor TEXT, value INTEGER)''')
        while self._running:
            try:
                message = self.queue.get(timeout=1)
            except:
                continue
            timestamp, sensor, value = message
            c.execute("INSERT INTO logs (timestamp, sensor, value) VALUES (?, ?, ?)", (timestamp, sensor, value))
            conn.commit()

class BaseSensor(Sensor):
    def __init__(self, name, queue, delay):
        super().__init__(name, queue)
        self.delay = delay

class FastSensor(BaseSensor):
    def __init__(self, name, queue):
        super().__init__(name, queue, delay=2)

class SlowSensor(BaseSensor):
    def __init__(self, name, queue):
        super().__init__(name, queue, delay=8)

if __name__ == '__main__':
    queue = Queue(maxsize=5)
    log = Log(queue, 'sensor_data.db')

    sensors = []
    for i in range(10):
        if i % 2 == 0:
            sensors.append(FastSensor(f'sensor{i}', queue))
        else:
            sensors.append(SlowSensor(f'sensor{i}', queue))

    log.start()
    for sensor in sensors:
        sensor.start()

    time.sleep(60)

    for sensor in sensors:
        sensor.stop()
    log.stop()

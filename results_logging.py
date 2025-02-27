import threading
import queue

class ResultLogger:
    def __init__(self):
        self.log_queue = queue.Queue()
        self.file_handle = None
        self.logging_thread = threading.Thread(target=self._write_logs)
        self.logging_thread.daemon = True
        self.logging_thread.start()

    def setup(self, filename):
        if self.file_handle is not None:
            self.file_handle.close()
        self.file_handle = open(filename, 'w')

    def _write_logs(self):
        while True:
            message = self.log_queue.get()
            if message is None:
                break
            if self.file_handle is not None:
                self.file_handle.write(message + '\n')
                self.file_handle.flush()
            self.log_queue.task_done()

    def log_connect(self, timestamp, benchmark_id, client_id):
        message = f"CONNECT#{timestamp}#{benchmark_id}#{client_id}"
        self.log_queue.put(message)

    def log_subscribe(self, timestamp, benchmark_id, client_id, topic_filter, purpose_filter, subscription_id):
        message = f"SUBSCRIBE#{timestamp}#{benchmark_id}#{client_id}#{topic_filter}#{purpose_filter}#{subscription_id}"
        self.log_queue.put(message)

    def log_publish(self, timestamp, benchmark_id, client_id, topic_name, purpose, msg_type, payload):
        message = f"PUBLISH#{timestamp}#{benchmark_id}#{client_id}#{topic_name}#{purpose}#{msg_type}#{payload}"
        self.log_queue.put(message)

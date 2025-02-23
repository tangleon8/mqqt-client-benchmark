import logging
import itertools
from enum import Enum
import paho.mqtt.client as mqtt
from paho.mqtt.enums import MQTTProtocolVersion, CallbackAPIVersion

class PurposeManagementMethod(Enum):
    PM_1 = "Purpose-Encoding Topics"
    PM_2 = "Per-Message Declaration"
    PM_3 = "Registration by Message"
    PM_4 = "Registration by Topic"

class C1RightsMethod(Enum):
    C1_1 = "Direct Publication"
    C1_2 = "Pre-Registration"

class C2RightsMethod(Enum):
    C2_1 = "Direct Publication"
    C2_2 = "Broker-Facilitated"

class C3RightsMethod(Enum):
    C3_1 = "Direct Publication"
    C4_2 = "Broker-Facilitated"

class BenchmarkClient:
    client_id: str
    mqtt_client: mqtt.Client
    logger: logging.Logger
    connect_waiting: bool = False

    class CorrectnessTestResults:
        client_id: str
        test_name: str
        success_count: int
        failure_count: int
        total_count: int
        failure_reasons: list[str]

        def __init__(self, client_id: str, test_name: str) -> None:
            self.client_id = client_id
            self.test_name = test_name
            self.success_count = 0
            self.failure_count = 0
            self.total_count = 0
            self.failure_reasons = list()

        def success(self) -> None:
            self.success_count += 1
            self.total_count += 1

        def failure(self, reason: str) -> None:
            self.failure_count += 1
            self.total_count += 1
            self.failure_reasons.append(reason)

        def get_results(self) -> str:
            result_str = f'Results for Client: {self.client_id} - Test: {self.test_name}\n'
            result_str += f'> Successes: {self.success_count} - Failures: {self.failure_count} - Total: {self.total_count}\n'
            for reason in self.failure_reasons:
                result_str += f'{reason}\n'
            return result_str

    current_correcness_test_results: CorrectnessTestResults

    def __init__(self, client_id: str, mqtt_version: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv5) -> None:
        self.client_id = client_id
        self.connect_waiting = False
        self.mqtt_client = mqtt.Client(
            callback_api_version=CallbackAPIVersion.VERSION2,
            client_id=client_id,
            protocol=mqtt_version,
            reconnect_on_failure=False
        )
        self.logger = logging.getLogger(client_id)
        self.mqtt_client.enable_logger(self.logger)

    def connect(self, broker_address: str, port: int = 1883, clean_start: bool = True) -> None:
        self.mqtt_client.on_connect = self._connect_callback
        self.mqtt_client.on_connect_fail = self._connect_fail_callback
        try:
            self.connect_waiting = True
            self.mqtt_client.connect(host=broker_address, port=port, clean_start=clean_start)
            self.mqtt_client.loop_start()
        except Exception as e:
            self.logger.error(f"{self.client_id} failed to connect to broker {broker_address}:{port}.")
            self.connect_waiting = False
            self.mqtt_client.loop_stop()

    def is_connected(self):
        return not self.connect_waiting and self.mqtt_client.is_connected()

    def _connect_callback(self, client, userdata, connect_flags, reason_code, properties):
        self.connect_waiting = False

    def _connect_fail_callback(self, client, userdata):
        self.connect_waiting = False
        self.logger.debug(msg=f"{self.client_id} - failed to connect to broker")

    def publish_with_purpose(self, topic: str, purpose: str, msg: str, method: PurposeManagementMethod) -> None:
        if method == PurposeManagementMethod.PM_1:
            purpose = purpose.replace('/', '|')
            purpose_subtopic = f'[{purpose}]'
            full_topic = f'{topic}/{purpose_subtopic}'
            self.mqtt_client.publish(topic=full_topic, payload=msg)
        elif method == PurposeManagementMethod.PM_2:
            properties = mqtt.Properties()
            properties.UserProperty = ('PF-MP', purpose)
            self.mqtt_client.publish(topic=topic, payload=msg, properties=properties)
        elif method == PurposeManagementMethod.PM_3:
            registration_topic = "$PF/purpose_management"
            properties = mqtt.Properties()
            properties.UserProperty = ('PF-MP', f"{purpose}:{topic}")
            self.mqtt_client.publish(topic=registration_topic, payload="", properties=properties)
            self.mqtt_client.publish(topic=topic, payload=msg)
        elif method == PurposeManagementMethod.PM_4:
            registration_topic = f"$PF/MP_reg/{topic}/[{purpose}]"
            self.mqtt_client.publish(topic=registration_topic, payload="")
            self.mqtt_client.publish(topic=topic, payload=msg)
        else:
            raise ValueError(f"Unknown method {method}")

    def subscribe_with_purpose(self, topic: str, purpose_filter: str, method: PurposeManagementMethod) -> None:
        if method == PurposeManagementMethod.PM_1:
            described_purposes = self.find_described_purposes(purpose_filter)
            topic_list = [(f'{topic}/[{purpose.replace("/", "|")}]', 0) for purpose in described_purposes]
            for subscription in topic_list:
                self.mqtt_client.subscribe(topic=subscription)
        elif method in [PurposeManagementMethod.PM_2, PurposeManagementMethod.PM_3]:
            properties = mqtt.Properties()
            properties.UserProperty = ('PF-SP', f"{purpose_filter}:{topic}")
            self.mqtt_client.subscribe(topic, properties=properties)
        elif method == PurposeManagementMethod.PM_4:
            registration_topic = f"$PF/SP_reg/{topic}/[{purpose_filter}]"
            self.mqtt_client.publish(registration_topic, payload="")
            self.mqtt_client.subscribe(topic)
        else:
            raise ValueError(f"Unknown method {method}")

    def purpose_management_correctness_message_callback(self, client, userdata, message):
        if message.payload.decode() == "GOOD":
            self.current_correcness_test_results.success()
        else:
            self.current_correcness_test_results.failure(f'Received message for non-approved purpose on {message.topic}')

    def initialize_test(self, test_name: str) -> None:
        self.current_correcness_test_results = self.CorrectnessTestResults(self.client_id, test_name)

    @staticmethod
    def find_described_purposes(purpose_filter: str) -> list[str]:
        filter_levels = purpose_filter.split('/')
        decomposed_levels = []
        for level in filter_levels:
            if '{' in level:
                level = level.replace('{', '').replace('}', '').split(',')
            else:
                level = [level]
            decomposed_levels.append(level)
        described_purposes = []
        for purpose_list in itertools.product(*decomposed_levels):
            purpose = '/'.join(purpose_list)
            if not './' in purpose:
                purpose = purpose.replace('/.', '')
                described_purposes.append(purpose)
        return described_purposes

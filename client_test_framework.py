import random
import time
import logging
from typing import List, Set, Dict
from dataclasses import dataclass

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class Client:
    id: int
    subscribed_topics: Set[str]
    is_connected: bool = True

class TestFramework:
    def __init__(self, num_clients: int, seed: int = None):
        # Set random seed for reproducibility
        if seed is None:
            seed = random.randint(0, 1000000)
        random.seed(seed)
        logger.info(f"Random seed for this test run: {seed}")
        
        self.clients: List[Client] = []
        self.topics = {"topic1", "topic2", "topic3"}  # Example topics
        self.purposes = {"chat", "alert", "update"}  # Example purposes
        self.create_clients(num_clients)

    def create_clients(self, num_clients: int) -> None:
        """Create specified number of clients with random topic subscriptions"""
        for i in range(num_clients):
            # Randomly select subset of topics for each client
            num_topics = random.randint(1, len(self.topics))
            subscribed_topics = set(random.sample(list(self.topics), num_topics))
            self.clients.append(Client(id=i, subscribed_topics=subscribed_topics))

    def publish_messages(self, messages_per_cycle: int) -> None:
        """Publish random messages from random clients to random topics"""
        active_clients = [c for c in self.clients if c.is_connected]
        if not active_clients:
            return
            
        for _ in range(messages_per_cycle):
            client = random.choice(active_clients)
            topic = random.choice(list(client.subscribed_topics))
            purpose = random.choice(list(self.purposes))
            logger.info(f"Client {client.id} publishing to {topic} with purpose {purpose}")

    def disconnect_clients(self, disconnect_rate: float) -> Set[int]:
        """Randomly disconnect clients based on rate (0.0-1.0)"""
        active_clients = [c for c in self.clients if c.is_connected]
        num_to_disconnect = int(len(active_clients) * disconnect_rate)
        clients_to_disconnect = random.sample(active_clients, min(num_to_disconnect, len(active_clients)))
        
        disconnected_ids = set()
        for client in clients_to_disconnect:
            client.is_connected = False
            disconnected_ids.add(client.id)
            logger.info(f"Disconnected client {client.id}")
        return disconnected_ids

    def reconnect_clients(self, disconnected_ids: Set[int], reconnect_rate: float) -> None:
        """Randomly reconnect a subset of disconnected clients"""
        disconnected_clients = [c for c in self.clients if not c.is_connected and c.id in disconnected_ids]
        num_to_reconnect = int(len(disconnected_clients) * reconnect_rate)
        clients_to_reconnect = random.sample(disconnected_clients, min(num_to_reconnect, len(disconnected_clients)))
        
        for client in clients_to_reconnect:
            client.is_connected = True
            logger.info(f"Reconnected client {client.id}")

    def invoke_rights(self, invocation_rate: float) -> Dict[int, bool]:
        """Have random clients invoke rights at specified rate"""
        active_clients = [c for c in self.clients if c.is_connected]
        num_to_invoke = int(len(active_clients) * invocation_rate)
        invoking_clients = random.sample(active_clients, min(num_to_invoke, len(active_clients)))
        
        rights_status = {}
        for client in invoking_clients:
            rights_status[client.id] = True  # Placeholder for actual right invocation
            logger.info(f"Client {client.id} invoking rights")
        return rights_status

    def process_rights(self, rights_status: Dict[int, bool], accept_rate: float) -> None:
        """Accept/Deny rights invocations randomly based on accept rate"""
        for client_id, _ in rights_status.items():
            is_accepted = random.random() < accept_rate
            logger.info(f"Client {client_id} rights {'accepted' if is_accepted else 'denied'}")

    def run_test_cycle(self, cycles: int, seconds_between: float = 1.0):
        """Run the test for specified number of cycles"""
        for cycle in range(cycles):
            logger.info(f"\nCycle {cycle + 1}")
            
            # Publish messages
            self.publish_messages(messages_per_cycle=random.randint(1, 5))
            
            # Disconnect clients
            disconnected = self.disconnect_clients(disconnect_rate=random.uniform(0.1, 0.3))
            
            # Reconnect clients
            self.reconnect_clients(disconnected, reconnect_rate=random.uniform(0.2, 0.5))
            
            # Invoke rights
            rights_status = self.invoke_rights(invocation_rate=random.uniform(0.1, 0.4))
            
            # Process rights
            self.process_rights(rights_status, accept_rate=random.uniform(0.3, 0.7))
            
            time.sleep(seconds_between)

# Example usage
if __name__ == "__main__":
    # Create test with 10 clients and specific seed for reproducibility
    test = TestFramework(num_clients=10, seed=42)
    test.run_test_cycle(cycles=5, seconds_between=1.0)

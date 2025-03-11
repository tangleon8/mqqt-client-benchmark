import random
import time
import logging
from typing import List, Set
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
        """1. Create specified number of clients with random topic subscriptions"""
        for i in range(num_clients):
            # Randomly select subset of topics for each client
            num_topics = random.randint(1, len(self.topics))
            subscribed_topics = set(random.sample(list(self.topics), num_topics))
            self.clients.append(Client(id=i, subscribed_topics=subscribed_topics))
            logger.info(f"Created client {i} subscribed to {subscribed_topics}")

    def subscribe_clients(self) -> None:
        """2. Subscribe clients to topics (already done in create_clients)"""
        # Note: Subscription to topics is handled in create_clients
        # This method is here for clarity but could be merged
        for client in self.clients:
            logger.info(f"Client {client.id} confirmed subscribed to {client.subscribed_topics}")

    def publish_messages(self, messages_per_cycle: int) -> None:
        """3. Publish messages through clients to topics and purposes"""
        active_clients = [c for c in self.clients if c.is_connected]
        if not active_clients:
            return
            
        for _ in range(messages_per_cycle):
            client = random.choice(active_clients)
            topic = random.choice(list(client.subscribed_topics))
            purpose = random.choice(list(self.purposes))
            logger.info(f"Client {client.id} publishing to {topic} with purpose {purpose}")

    def disconnect_clients(self, disconnect_rate: float) -> None:
        """4. Disconnect a set of clients"""
        active_clients = [c for c in self.clients if c.is_connected]
        num_to_disconnect = int(len(active_clients) * disconnect_rate)
        clients_to_disconnect = random.sample(active_clients, min(num_to_disconnect, len(active_clients)))
        
        for client in clients_to_disconnect:
            client.is_connected = False
            logger.info(f"Disconnected client {client.id}")

    def run_test_cycle(self, cycles: int, seconds_between: float = 1.0):
        """Run test for specified cycles with disconnect every X seconds"""
        for cycle in range(cycles):
            logger.info(f"\nCycle {cycle + 1}")
            
            # Publish messages
            self.publish_messages(messages_per_cycle=random.randint(1, 5))
            
            # Disconnect clients every X seconds
            self.disconnect_clients(disconnect_rate=random.uniform(0.1, 0.3))
            
            time.sleep(seconds_between)

# Example usage
if __name__ == "__main__":
    # Create test with 10 clients and specific seed for reproducibility
    test = TestFramework(num_clients=10, seed=42)
    test.subscribe_clients()  # Explicit subscription confirmation
    test.run_test_cycle(cycles=5, seconds_between=1.0)

"""
Replay buffer for the PPO path-selection agent.

Stores (state, action, reward, next_state, done) transitions and samples
uniform random mini-batches for the policy update.
"""

import numpy as np
from collections import deque
import random


class ReplayBuffer:
    """
    Replay buffer for storing and sampling experience transitions.

    Attributes:
        buffer (deque): Double-ended queue to store transitions
        batch_size (int): Size of sampled batches
    """

    def __init__(self, buffer_size=10000, batch_size=64):
        """
        Initialize the replay buffer.

        Args:
            buffer_size (int): Maximum size of the buffer
            batch_size (int): Size of sampled batches
        """
        self.buffer = deque(maxlen=buffer_size)
        self.batch_size = batch_size

    def add(self, state, action, reward, next_state, done):
        """
        Add a transition to the buffer.

        Args:
            state (np.ndarray): Current state
            action (int): Action taken
            reward (float): Reward received
            next_state (np.ndarray): Next state
            done (bool): Whether the episode is done
        """
        if isinstance(action, (int, float)):
            action = np.array([action])
        self.buffer.append((state, action, reward, next_state, done))

    def sample(self):
        """
        Sample a batch of transitions from the buffer.

        Returns:
            tuple: (states, actions, rewards, next_states, dones)
        """
        batch = random.sample(self.buffer, min(self.batch_size, len(self.buffer)))
        states, actions, rewards, next_states, dones = zip(*batch)

        states = np.array(states)
        actions = np.array(actions)
        rewards = np.array(rewards).reshape(-1, 1)
        next_states = np.array(next_states)
        dones = np.array(dones).reshape(-1, 1)

        return states, actions, rewards, next_states, dones

    def size(self):
        """
        Get the current size of the buffer.

        Returns:
            int: Number of transitions in the buffer
        """
        return len(self.buffer)

    def clear(self):
        """
        Clear the buffer.
        """
        self.buffer.clear()

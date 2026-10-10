"""
Normalizer utilities for state feature normalization.

This module provides a Normalizer class that maintains running statistics
to normalize path features, improving training stability for the PPO agent.
"""

import numpy as np


class Normalizer:
    """
    Feature normalizer for state inputs.

    This class maintains running statistics to normalize state features,
    which helps with training stability.

    Attributes:
        mean (np.ndarray): Running mean of each feature
        var (np.ndarray): Running variance of each feature
        count (int): Number of samples seen so far
        eps (float): Small value to avoid division by zero
    """

    def __init__(self, n_features, eps=1e-8):
        """
        Initialize the normalizer.

        Args:
            n_features (int): Number of features in the state
            eps (float): Small value to avoid division by zero
        """
        self.mean = np.zeros(n_features, dtype=np.float32)
        self.var = np.ones(n_features, dtype=np.float32)
        self.count = 0
        self.eps = eps

    def update(self, x):
        """
        Update running statistics with a batch of samples.

        Args:
            x (np.ndarray): Batch of samples with shape (batch_size, n_features)
        """
        if len(x.shape) == 1:
            x = x.reshape(1, -1)

        batch_size = x.shape[0]

        # Compute batch mean and variance
        batch_mean = np.mean(x, axis=0)
        batch_var = np.var(x, axis=0)

        # Update running mean and variance
        new_count = self.count + batch_size
        delta = batch_mean - self.mean

        self.mean = self.mean + delta * batch_size / new_count
        m_a = self.var * self.count
        m_b = batch_var * batch_size
        M2 = m_a + m_b + np.square(delta) * self.count * batch_size / new_count
        self.var = M2 / new_count
        self.count = new_count

    def normalize(self, x):
        """
        Normalize a batch of samples.

        Args:
            x (np.ndarray): Batch of samples with shape (batch_size, n_features)
                            or single sample with shape (n_features,)

        Returns:
            np.ndarray: Normalized samples
        """
        # Update statistics
        self.update(x)

        # Normalize
        return (x - self.mean) / (np.sqrt(self.var) + self.eps)

    def get_state(self):
        """
        Get the current state of the normalizer.

        Returns:
            dict: Dictionary with current statistics
        """
        return {
            'mean': self.mean,
            'var': self.var,
            'count': self.count
        }

    def set_state(self, state):
        """
        Set the state of the normalizer.

        Args:
            state (dict): Dictionary with statistics
        """
        self.mean = state['mean']
        self.var = state['var']
        self.count = state['count']

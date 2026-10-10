"""
Actor-Critic network for the PPO path-selection agent.

Two shared ReLU feature layers feed an actor head (a linear layer with
`action_dim` logits, softmaxed over the last dimension in `forward`) and a
critic head (a linear layer giving the state value).
"""

import torch
import torch.nn as nn
import torch.nn.functional as F


class ActorCriticNetwork(nn.Module):
    """
    Combined Actor-Critic network for the PPO algorithm.

    Attributes:
        hidden_dim (int): Dimension of hidden layers
    """

    def __init__(self, state_dim, action_dim, hidden_dim=32):
        """
        Initialize the Actor-Critic network.

        Args:
            state_dim (int): Dimension of the state input
            action_dim (int): Dimension of the action output (number of paths)
            hidden_dim (int): Dimension of hidden layers
        """
        super(ActorCriticNetwork, self).__init__()

        self.hidden_dim = hidden_dim

        self.feature_layers = nn.Sequential(
            nn.Linear(state_dim, hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, hidden_dim),
            nn.ReLU()
        )

        # Actor head: logits, softmaxed in forward()
        self.actor_head = nn.Sequential(
            nn.Linear(hidden_dim, action_dim),
        )

        # Critic head (value network)
        self.critic_head = nn.Linear(hidden_dim, 1)

    def forward(self, state):
        """
        Forward pass through the network.

        Args:
            state (torch.Tensor): Input state tensor

        Returns:
            tuple: (action_probs, value)
                - action_probs: Probabilities for each action
                - value: Estimated state value
        """
        features = self.feature_layers(state)
        action_logits = self.actor_head(features)
        action_probs = F.softmax(action_logits, dim=-1)
        value = self.critic_head(features)
        return action_probs, value

    def get_value(self, state):
        """
        Get value estimate for a state.

        Args:
            state (torch.Tensor): Input state tensor

        Returns:
            torch.Tensor: Estimated state value
        """
        features = self.feature_layers(state)
        return self.critic_head(features)

    def get_entropy(self, state):
        """
        Calculate entropy of the policy for a state.

        Args:
            state (torch.Tensor): Input state tensor

        Returns:
            torch.Tensor: Entropy of the policy
        """
        features = self.feature_layers(state)
        action_logits = self.actor_head(features)
        action_probs = F.softmax(action_logits, dim=-1)
        return -torch.sum(action_probs * torch.log(action_probs + 1e-10), dim=-1)

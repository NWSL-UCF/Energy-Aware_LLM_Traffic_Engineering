"""
PPO agent for path selection (Agent1).

The agent scores every candidate path from its state tuple
(path_power_norm, ALU, MLU, delay_rank) and returns a probability per path.
Once per controller cycle it receives the per-path rewards for the previous
decision, stores the transitions and, when enough samples are buffered,
performs a clipped-PPO update. Training scalars are appended to a CSV under
`RL/training_metrics/`.
"""

import torch
import torch.nn as nn
import torch.optim as optim
import numpy as np
from .models import ActorCriticNetwork
from ..memory.replay_buffer import ReplayBuffer
from ..utils.normalizer import Normalizer
from datetime import datetime
import csv
import logging
import os

LOG = logging.getLogger('ryu.app.ppo_agent')


class PPOAgent:
    """
    Proximal Policy Optimization (PPO) agent for path selection.

    1. select_action: called once per agent cycle, returns a probability per path
    2. update: called on the next cycle with the rewards earned by those paths

    Attributes:
        state_dim (int): Dimension of the state (path features)
        action_dim (int): Number of possible paths to choose from
        device (torch.device): Device to run the model on (CPU or GPU)
        actor_critic (ActorCriticNetwork): Neural network for policy and value functions
        optimizer (torch.optim.Optimizer): Optimizer for training the network
        normalizer (Normalizer): State feature normalizer
        buffer (ReplayBuffer): Experience replay buffer
        clip_epsilon (float): PPO clipping parameter
        gamma (float): Discount factor
        gae_lambda (float): GAE lambda parameter
        entropy_coef (float): Entropy coefficient for exploration
        value_loss_coef (float): Value loss coefficient
        max_grad_norm (float): Maximum gradient norm for clipping
        batch_size (int): Batch size for training
        update_epochs (int): Number of epochs to update per batch
        prev_path_stats (dict): Path statistics of the last decision; select_action
            is the single writer, update() only reads it
    """

    def __init__(self, config):
        """
        Initialize the PPO agent with configuration parameters.

        Args:
            config (dict): Configuration parameters for the agent
        """
        self.state_dim = config['state_dim']
        self.action_dim = config['action_dim']
        self.device = torch.device(config['device'])

        self.actor_critic = ActorCriticNetwork(
            state_dim=self.state_dim,
            action_dim=self.action_dim,
            hidden_dim=config['hidden_dim']
        ).to(self.device)

        self.optimizer = optim.Adam(
            self.actor_critic.parameters(),
            lr=config['learning_rate']
        )

        self.normalizer = Normalizer(self.state_dim)

        self.buffer = ReplayBuffer(
            buffer_size=config['buffer_size'],
            batch_size=config['batch_size']
        )

        # PPO parameters
        self.clip_epsilon = config['clip_epsilon']
        self.gamma = config['gamma']
        self.gae_lambda = config['gae_lambda']
        self.entropy_coef = config['entropy_coef']
        self.value_loss_coef = config['value_loss_coef']
        self.max_grad_norm = config['max_grad_norm']
        self.batch_size = config['batch_size']
        self.update_epochs = config['update_epochs']

        # Path stats of the last decision, paired with the next cycle's rewards
        self.prev_path_stats = None

        # Training-metrics CSV, one row per optimizer step
        self.timestamp = datetime.now().strftime("%m-%d_%H-%M-%S")
        self.csv_path = f"RL/training_metrics/training_metrics_{self.timestamp}.csv"
        self.global_step = 0
        os.makedirs(os.path.dirname(self.csv_path), exist_ok=True)
        with open(self.csv_path, "w", newline="") as f:
            csv.writer(f).writerow([
                "step",
                "loss_total",
                "loss_policy",
                "loss_value",
                "policy_entropy",
                "reward_mean",
                "clip_fraction"
            ])

    def preprocess_state(self, path_stats):
        """
        Convert path statistics into raw (un-normalized) state vectors.

        Args:
            path_stats (dict): Dictionary with path statistics
                              {path_id: (path_power_norm, ALU, MLU, delay_rank)}

        Returns:
            dict: Dictionary mapping path_ids to their state vectors
            list: List of path_ids in the same order as the states
        """
        path_ids = list(path_stats.keys())
        states = {}
        for path_id, stats in path_stats.items():
            states[path_id] = np.array(stats, dtype=np.float32)
        return states, path_ids

    def select_action(self, path_stats):
        """
        Select paths based on their statistics using the trained policy.

        Args:
            path_stats (dict): Dictionary with path statistics
                            {path_id: (path_power_norm, ALU, MLU, delay_rank)}

        Returns:
            dict: Dictionary mapping path_ids to their selection probabilities
        """
        # Keep our own copy of the decision state; update() pairs it with the next state.
        self.prev_path_stats = dict(path_stats)

        path_ids = list(path_stats.keys())
        features = []
        for path_id in path_ids:
            state = np.array(path_stats[path_id], dtype=np.float32)
            state = self.normalizer.normalize(state)
            features.append(state)

        features_tensor = torch.FloatTensor(np.array(features)).to(self.device)

        with torch.no_grad():
            features = self.actor_critic.feature_layers(features_tensor)
            logits = self.actor_critic.actor_head(features)
            # Softmax across the candidate paths of this call
            probabilities = torch.nn.functional.softmax(logits, dim=0).cpu().numpy()

        path_probs = {}
        for i, path_id in enumerate(path_ids):
            path_probs[path_id] = float(probabilities[i, 0])

        return path_probs

    def update(self, path_rewards, current_path_stats):
        """
        Update the agent with the rewards earned by the paths of the last decision.

        Args:
            path_rewards (dict): Dictionary mapping path_ids to their rewards
            current_path_stats (dict): Current path statistics, paired as the
                                       next state of the previous decision
        """
        if self.prev_path_stats is None:
            return

        # Grow the action space when new paths appear
        current_path_count = len(current_path_stats)
        if current_path_count > self.action_dim:
            self.update_action_space(current_path_count)

        prev_states, prev_path_ids = self.preprocess_state(self.prev_path_stats)
        current_states, _ = self.preprocess_state(current_path_stats)
        # path_id -> action index, built once
        idx_of = {pid: i for i, pid in enumerate(prev_path_ids)}

        # Add one transition per rewarded path
        for path_id, reward in path_rewards.items():
            if path_id not in prev_states:
                continue

            state = prev_states[path_id]

            next_state = current_states.get(path_id, None)
            if next_state is None:
                next_state = np.zeros_like(state)
                done = True
            else:
                done = False

            action = idx_of.get(path_id)
            if action is None:
                continue
            if action >= self.action_dim:
                LOG.warning("[PPO_Agent1]: Action %d is out of bounds for action space %d",
                            action, self.action_dim)
                continue

            self.buffer.add(state, action, reward, next_state, done)

        if self.buffer.size() >= self.batch_size:
            self._update_policy()

    def _update_policy(self):
        """
        Update the policy using the PPO algorithm.
        """
        states, actions, rewards, next_states, dones = self.buffer.sample()

        states = torch.FloatTensor(states).to(self.device)
        rewards = torch.FloatTensor(rewards).to(self.device)
        next_states = torch.FloatTensor(next_states).to(self.device)
        dones = torch.FloatTensor(dones).to(self.device)

        # Drop actions that are out of bounds for the current action space
        valid_indices = []
        for i, action in enumerate(actions):
            if action < self.action_dim:
                valid_indices.append(i)

        if len(valid_indices) == 0:
            LOG.warning("[PPO_Agent1]: No valid actions found for update")
            return

        valid_indices = torch.tensor(valid_indices, device=self.device)
        states = states.index_select(0, valid_indices)
        actions_np = np.array(actions)
        actions = torch.LongTensor(actions_np[valid_indices.cpu()].astype(int)).to(self.device)
        rewards = rewards.index_select(0, valid_indices)
        next_states = next_states.index_select(0, valid_indices)
        dones = dones.index_select(0, valid_indices)

        actions = actions.view(-1, 1)

        # Old action probabilities, values and GAE targets
        with torch.no_grad():
            old_action_probs, old_values = self.actor_critic(states)
            old_action_probs = old_action_probs.gather(1, actions)
            next_values = self.actor_critic.get_value(next_states)
            returns, advantages = self._compute_gae(rewards, old_values, next_values, dones)

        for _ in range(self.update_epochs):
            action_probs, values = self.actor_critic(states)
            action_probs = action_probs.gather(1, actions)

            # Clipped surrogate objective
            ratio = action_probs / (old_action_probs + 1e-8)
            surr1 = ratio * advantages
            surr2 = torch.clamp(ratio, 1.0 - self.clip_epsilon, 1.0 + self.clip_epsilon) * advantages
            policy_loss = -torch.min(surr1, surr2).mean()

            value_loss = 0.5 * (returns - values).pow(2).mean()

            entropy = self.actor_critic.get_entropy(states).mean()

            loss = policy_loss + self.value_loss_coef * value_loss - self.entropy_coef * entropy

            self.optimizer.zero_grad()
            loss.backward()
            nn.utils.clip_grad_norm_(self.actor_critic.parameters(), self.max_grad_norm)
            self.optimizer.step()

            self.global_step += 1
            self._log_scalars_csv(
                loss_total=loss.item(),
                loss_policy=policy_loss.item(),
                loss_value=value_loss.item(),
                policy_entropy=entropy.item(),
                reward_mean=rewards.mean().item(),
                clip_fraction=((ratio < 1-self.clip_epsilon) | (ratio > 1+self.clip_epsilon)).float().mean().item(),
            )

    def _compute_gae(self, rewards, values, next_values, dones):
        """
        Compute returns and advantages using Generalized Advantage Estimation.

        Args:
            rewards (torch.Tensor): Batch of rewards
            values (torch.Tensor): Batch of value estimates
            next_values (torch.Tensor): Batch of next value estimates
            dones (torch.Tensor): Batch of done flags

        Returns:
            tuple: (returns, advantages)
        """
        batch_size = rewards.size(0)
        returns = torch.zeros_like(rewards)
        advantages = torch.zeros_like(rewards)

        last_gae_lam = 0

        for t in reversed(range(batch_size)):
            if t == batch_size - 1:
                next_value = next_values[t]
            else:
                next_value = values[t + 1]

            if dones[t]:
                delta = rewards[t] - values[t]
                last_gae_lam = delta
            else:
                delta = rewards[t] + self.gamma * next_value - values[t]
                last_gae_lam = delta + self.gamma * self.gae_lambda * last_gae_lam

            advantages[t] = last_gae_lam
            returns[t] = advantages[t] + values[t]

        return returns, advantages

    def save_model(self, path):
        """
        Save the model to the given path.

        Args:
            path (str): Path to save the model
        """
        torch.save({
            'action_dim': self.action_dim,
            'actor_critic_state_dict': self.actor_critic.state_dict(),
            'optimizer_state_dict': self.optimizer.state_dict(),
            'normalizer_state': self.normalizer.get_state(),
        }, path)

    def load_model(self, path):
        """
        Load the model from the given path.

        The action space is resized to the checkpoint's `action_dim` before the
        state dicts are loaded, so a checkpoint saved after the action space
        grew can be restored.

        Args:
            path (str): Path to load the model from
        """
        checkpoint = torch.load(path)
        state_dict = checkpoint['actor_critic_state_dict']
        action_dim = checkpoint['action_dim']
        if action_dim != self.action_dim:
            self.update_action_space(action_dim)
        self.actor_critic.load_state_dict(state_dict)
        self.optimizer.load_state_dict(checkpoint['optimizer_state_dict'])
        self.normalizer.set_state(checkpoint['normalizer_state'])

    def update_action_space(self, new_action_dim):
        """
        Resize the actor head to a new number of candidate paths.

        Args:
            new_action_dim (int): New number of possible paths
        """
        new_actor_critic = ActorCriticNetwork(
            state_dim=self.state_dim,
            action_dim=new_action_dim,
            hidden_dim=self.actor_critic.hidden_dim
        ).to(self.device)

        # Transfer everything except the actor head
        shared_dict = {k: v for k, v in self.actor_critic.state_dict().items()
                       if 'actor_head' not in k}

        model_dict = new_actor_critic.state_dict()
        model_dict.update(shared_dict)
        new_actor_critic.load_state_dict(model_dict)

        self.actor_critic = new_actor_critic
        self.optimizer = optim.Adam(
            self.actor_critic.parameters(),
            lr=self.optimizer.param_groups[0]['lr']
        )

        self.action_dim = new_action_dim

        # Buffered actions index the old action space
        self.buffer.clear()

    def _log_scalars_csv(self, **metrics):
        """Append one training step to the CSV."""
        with open(self.csv_path, "a", newline="") as f:
            csv.writer(f).writerow([
                self.global_step,
                metrics["loss_total"],
                metrics["loss_policy"],
                metrics["loss_value"],
                metrics["policy_entropy"],
                metrics["reward_mean"],
                metrics["clip_fraction"]
            ])

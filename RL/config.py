"""
Hyperparameters for the PPO path-selection agent (Agent1).

`get_config()` returns a plain dict; the agent grows `action_dim` at runtime
to the number of candidate paths.
"""

CONFIG = {
    # State and action dimensions
    'state_dim': 4,     # (path_power_norm, ALU, MLU, delay_rank)
    'action_dim': 10,   # grown at runtime to the number of available paths

    # Network architecture
    'hidden_dim': 32,
    'device': 'cpu',    # 'cpu' or 'cuda'

    # PPO parameters
    'learning_rate': 3e-4,
    'gamma': 0.99,
    'gae_lambda': 0.95,
    'clip_epsilon': 0.2,
    'entropy_coef': 0.01,
    'value_loss_coef': 0.5,
    'max_grad_norm': 0.5,

    # Training parameters
    'buffer_size': 5000,
    'batch_size': 32,
    'update_epochs': 6,
}


def get_config():
    """
    Return a copy of the agent configuration.

    Returns:
        dict: Configuration parameters
    """
    return CONFIG.copy()

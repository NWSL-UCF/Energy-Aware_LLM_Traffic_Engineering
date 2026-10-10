"""Ryu events exchanged between the controller apps."""
from ryu.controller.event import EventBase


class EventDelayUpdate(EventBase):
    """Sent by network_delay with the latest per-link delay table; consumed by network_monitor."""

    def __init__(self, delay_table):
        super(EventDelayUpdate, self).__init__()
        self.delay_table = delay_table
